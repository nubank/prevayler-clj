(ns prevayler-clj-aws.core
  (:require
   [base64-clj.core :as base64]
   [clojure.java.io :as io]
   [cognitect.aws.client.api :as aws]
   [prevayler-clj.prevayler4 :refer [Prevayler]]
   [taoensso.nippy :as nippy])
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

(defn aws-invoke
  [client op-map]
  (let [result (aws/invoke client op-map)]
    (if (:cognitect.anomalies/category result)
      (throw (ex-info (:message result) result))
      result)))

(defn- snapshot-exists? [s3-client bucket snapshot-path]
  (->> (aws-invoke s3-client {:op :ListObjects
                              :request {:Bucket bucket
                                        :Prefix snapshot-path}})
       :Contents
       (some #(= snapshot-path (:Key %)))))

(defn- read-snapshot [s3-client bucket snapshot-path]
  (if (snapshot-exists? s3-client bucket snapshot-path)
    (-> (aws-invoke s3-client {:op :GetObject
                               :request {:Bucket bucket
                                         :Key snapshot-path}})
        :Body
        unmarshal)
    {:partkey 0}))

(defn- save-snapshot! [s3-client bucket snapshot-path snapshot]
  (aws-invoke s3-client {:op :PutObject
                         :request {:Bucket bucket
                                   :Key snapshot-path
                                   :Body (marshal snapshot)}}))

; TODO follow Last Evaluated Key
(defn- read-items [client table partkey]
  (try
    (->> (get-in
          (aws-invoke client {:op :Query
                              :request {:TableName table
                                        :KeyConditionExpression "partkey = :partkey"
                                        :ExpressionAttributeValues {":partkey" {:S (str partkey)}}}})
          [:Items])
         (map (comp :B :content))
         (map unmarshal))
    (catch Exception e
      (.printStackTrace e)
      [])))

(defn- restore-events! [handler state-atom client table partkey]
  (let [items (read-items client table partkey)]
    (doseq [[timestamp event] items]
      (swap! state-atom handler event timestamp))))

(defn- write-event! [client table partkey [timestamp :as value]]
  (aws-invoke client {:op :PutItem
                      :request {:TableName table
                                :Item {:partkey {:S (str partkey)}
                                       :timestamp {:N (str timestamp)}
                                       :content {:B (marshal value)}}}}))

(defn prevayler! [{:keys [initial-state business-fn timestamp-fn aws-opts]
                   :or {initial-state {}
                        timestamp-fn #(System/currentTimeMillis)}}]
  (let [client (aws/client (merge (:aws-cli-opts aws-opts) {:api :dynamodb}))
        s3-client (aws/client (merge (:aws-cli-opts aws-opts) {:api :s3}))
        table (:dynamodb-table aws-opts)
        snapshot-path (or (:snapshot-path aws-opts) "snapshot")
        bucket (:s3-bucket aws-opts)
        {state :state old-partkey :partkey} (read-snapshot s3-client bucket snapshot-path)
        state-atom (atom (or state initial-state))
        new-partkey (inc old-partkey)]

    (restore-events! business-fn state-atom client table old-partkey)

    ; since s3 update is atomic, if saving snapshot fails next prevayler will pick the previous state
    ; and restore events from the previous partkey
    (save-snapshot! s3-client bucket snapshot-path {:state @state-atom :partkey new-partkey})

    (reify
      Prevayler
      (handle! [this event]
        (locking this ; (I)solation: strict serializability.
          (let [timestamp (timestamp-fn)
                new-state (business-fn @state-atom event timestamp)] ; (C)onsistency: must be guaranteed by the handler. The event won't be journalled when the handler throws an exception.)
            (write-event! client table new-partkey [timestamp event]) ; (D)urability
            (reset! state-atom new-state)))) ; (A)tomicity
      (timestamp [_] (timestamp-fn))

      IDeref (deref [_] @state-atom)

      Closeable (close [_] (aws/stop client)))))
