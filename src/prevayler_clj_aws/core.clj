(ns prevayler-clj-aws.core
  (:require
    [base64-clj.core :as base64]
    [clojure.java.io :as io]
    [cognitect.aws.client.api :as aws]
    [prevayler-clj.prevayler4 :refer [Prevayler]]
    [taoensso.nippy :as nippy]
    [prevayler-clj-aws.util :as util])
  (:import
    [clojure.lang IDeref]
    [java.io ByteArrayOutputStream Closeable]))

(defn- marshal [value]
  (-> (nippy/freeze value)
      base64/encode-bytes
      String.))

(defn- unmarshal [in]
  (-> (with-open [out (ByteArrayOutputStream.)]
        (io/copy in out)
        (.toByteArray out))
      base64/decode-bytes
      nippy/thaw))

(defn- snapshot-exists? [s3-cli bucket snapshot-path]
  (->> (util/aws-invoke s3-cli {:op      :ListObjects
                                :request {:Bucket bucket
                                          :Prefix snapshot-path}})
       :Contents
       (some #(= snapshot-path (:Key %)))))

(defn- snapshot-v2-path [snapshot-path]
  (str snapshot-path "-v2"))

(defn- unmarshal-from-in [in]
  (-> in
      java.io.BufferedInputStream.
      java.io.DataInputStream.
      nippy/thaw-from-in!))

(defn- read-snapshot [s3-cli bucket snapshot-path]
  (let [v2-path (snapshot-v2-path snapshot-path)]
    (if (snapshot-exists? s3-cli bucket v2-path)
      (-> (util/aws-invoke s3-cli {:op      :GetObject
                                   :request {:Bucket bucket
                                             :Key    v2-path}})
          :Body
          unmarshal-from-in)
      (if (snapshot-exists? s3-cli bucket snapshot-path)
        (-> (util/aws-invoke s3-cli {:op      :GetObject
                                     :request {:Bucket bucket
                                               :Key    snapshot-path}})
            :Body
            unmarshal)
        {:partkey 0}))))

(defn- marshal-to-in [value]
  (let [input  (java.io.PipedInputStream. (* 1024 32)) ; 32k buffer
        output (-> input java.io.PipedOutputStream. java.io.BufferedOutputStream. java.io.DataOutputStream.)]
      (future
        (with-open [out output]
          (nippy/freeze-to-out! out value)))
      input))

(defn- save-snapshot! [s3-cli bucket snapshot-path snapshot]
  (let [v2-path (snapshot-v2-path snapshot-path)]
    (util/aws-invoke s3-cli {:op      :PutObject
                             :request {:Bucket bucket
                                       :Key    v2-path
                                       :Body   (marshal-to-in snapshot)}})))

(defn- read-items [dynamo-cli table partkey page-size]
  (letfn [(read-page [exclusive-start-key]
            (let [result (util/aws-invoke
                          dynamo-cli
                          {:op      :Query
                           :request {:TableName                 table
                                     :KeyConditionExpression    "partkey = :partkey"
                                     :ExpressionAttributeValues {":partkey" {:N (str partkey)}}
                                     :Limit                     page-size
                                     :ExclusiveStartKey         exclusive-start-key}})
                  {items :Items last-key :LastEvaluatedKey} result]
              (println (count items) "items read. last-key:" last-key)

              (lazy-cat (map (comp unmarshal :B :content) items)
                        (if (seq last-key)
                          (read-page last-key)
                          []))))]
    (read-page {:order {:N "0"} :partkey {:N (str partkey)}})))

(defn- print-when-slow [f msg]
  (let [t0 (System/currentTimeMillis)
        result (f)
        ellapsed (- (System/currentTimeMillis) t0)]
    (when (> ellapsed 1000)
      (println "Handling took" ellapsed "ms:" msg))
    result))

(defn- restore-events! [dynamo-cli handler state-atom table partkey page-size]
  (let [items (read-items dynamo-cli table partkey page-size)]
    (doseq [[timestamp event expected-state-hash] items]
      (let [state (print-when-slow #(swap! state-atom handler event timestamp)
                                   event)]
        (when (and expected-state-hash                      ; Old journals don't have this state hash saved (2023-10-25)
                   (not= (hash state) expected-state-hash))
          (println "Inconsistent state detected after restoring event:\n" event)
          (throw (IllegalStateException. "Inconsistent state detected during event journal replay. https://github.com/klauswuestefeld/prevayler-clj/blob/master/reference.md#inconsistent-state-detected")))))))

(defn- write-event! [dynamo-cli table partkey order event]
  (util/aws-invoke dynamo-cli {:op      :PutItem
                               :request {:TableName table
                                         :Item      {:partkey {:N (str partkey)}
                                                     :order   {:N (str order)}
                                                     :content {:B (marshal event)}}}}))

(defn prevayler!
  "Creates a new prevayler instance.
   Receives a map with the following attributes:

   `:initial-state` (optional): the initial state when creating the instance for the first time, default is an empty map
   `:business-fn`: a function that receives current state, event and timestamp and returns the next state
   `:aws-opts`: a map as describe below

   `:aws-opts`: is a map with the following attributes:

   `:dynamodb-table`: the name of the dynamodb table where the journal will be stored
   `:s3-bucket`: the name of the bucket where the snapshot will be stored
   `:snapshot-path` (optional): the path inside the s3-bucket where the snapshot will be stored, default is \"snapshot\""
  [{:keys [initial-state business-fn timestamp-fn aws-opts]
    :or   {initial-state {}
           timestamp-fn  #(System/currentTimeMillis)}}]
  (let [{:keys [dynamodb-client s3-client dynamodb-table snapshot-path s3-bucket page-size]
         :or   {dynamodb-client (aws/client {:api :dynamodb})
                s3-client       (aws/client {:api :s3})
                snapshot-path   "snapshot"
                page-size       1000}} aws-opts
        _ (println "Reading snapshot bucket...")
        {state :state snapshot-index :partkey} (read-snapshot s3-client s3-bucket snapshot-path)
        _ (println "Reading snapshot bucket done.")
        state-atom (atom (or state initial-state))
        snapshot-index-atom (atom snapshot-index)]
    
    (println "Restoring events...")
    (restore-events! dynamodb-client business-fn state-atom dynamodb-table @snapshot-index-atom page-size)
    (println "Restoring events done.")

    (let [order-atom (atom 0)
          snapshot-fn! (fn []
                         (println "Saving snapshot to bucket...")
                         ; Since s3 update is atomic, if saving snapshot fails next prevayler will pick the previous state
                         ; and restore events from the previous partkey
                         (save-snapshot! s3-client s3-bucket snapshot-path {:state @state-atom
                                                                            :partkey (inc @snapshot-index-atom)})
                         (println "Snapshot done.")
                         (swap! snapshot-index-atom inc)
                         (reset! order-atom 0))]
      
      (snapshot-fn!)

      (reify
        Prevayler
        (handle! [this event]
          (locking this ; (I)solation: strict serializability.
            (let [current-state @state-atom
                  timestamp (timestamp-fn)
                  new-state (business-fn current-state event timestamp)] ; (C)onsistency: must be guaranteed by the handler. The event won't be journalled when the handler throws an exception.)
              (when-not (identical? new-state current-state)
                (write-event! dynamodb-client dynamodb-table @snapshot-index-atom
                              (swap! order-atom inc) ; Skips an order number if there is an exception, but that's OK.
                              [timestamp event (hash new-state)]) ; (D)urability
                (reset! state-atom new-state)) ; (A)tomicity
              new-state)))

        (snapshot! [this]
          (locking this
            (snapshot-fn!)))

        (timestamp [_] (timestamp-fn))

        IDeref (deref [_] @state-atom)

        Closeable (close [_] (aws/stop dynamodb-client))))))
