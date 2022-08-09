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

(defn- snapshot-exists? [s3-client bucket snapshot-path]
  (->> (util/aws-invoke s3-client {:op :ListObjects
                                   :request {:Bucket bucket
                                             :Prefix snapshot-path}})
       :Contents
       (some #(= snapshot-path (:Key %)))))

(defn- read-snapshot [s3-client bucket snapshot-path]
  (if (snapshot-exists? s3-client bucket snapshot-path)
    (-> (util/aws-invoke s3-client {:op :GetObject
                                    :request {:Bucket bucket
                                              :Key snapshot-path}})
        :Body
        unmarshal)
    {:partkey 0}))

(defn- save-snapshot! [s3-client bucket snapshot-path snapshot]
  (util/aws-invoke s3-client {:op :PutObject
                              :request {:Bucket bucket
                                        :Key snapshot-path
                                        :Body (marshal snapshot)}}))

(defn- read-items [client table partkey page-size]
  (letfn [(read-page [exclusive-start-key]
            (let [result (util/aws-invoke
                          client
                          {:op :Query
                           :request {:TableName table
                                     :KeyConditionExpression "partkey = :partkey"
                                     :ExpressionAttributeValues {":partkey" {:S (str partkey)}}
                                     :Limit page-size
                                     :ExclusiveStartKey exclusive-start-key}})
                  {items :Items last-key :LastEvaluatedKey} result]
              (lazy-cat
               (map (comp unmarshal :B :content) items)
               (if (seq last-key)
                 (read-page last-key)
                 []))))]
    (read-page {:order {:N "0"} :partkey {:S (str partkey)}})))

(defn- restore-events! [handler state-atom client table partkey page-size]
  (let [items (read-items client table partkey page-size)]
    (doseq [[timestamp event] items]
      (swap! state-atom handler event timestamp))))

(defn- write-event! [client table partkey order event]
  (util/aws-invoke client {:op :PutItem
                           :request {:TableName table
                                     :Item {:partkey {:S (str partkey)}
                                            :order {:N (str order)}
                                            :content {:B (marshal event)}}}}))

(defn prevayler! [{:keys [initial-state business-fn timestamp-fn aws-opts]
                   :or {initial-state {}
                        timestamp-fn #(System/currentTimeMillis)}}]
  (let [dynamodb-cli (or (:dynamodb-client aws-opts) (aws/client {:api :dynamodb}))
        s3-cli (or (:s3-client aws-opts) (aws/client {:api :s3-client}))
        table (:dynamodb-table aws-opts)
        snapshot-path (or (:snapshot-path aws-opts) "snapshot")
        bucket (:s3-bucket aws-opts)
        {state :state old-partkey :partkey} (read-snapshot s3-cli bucket snapshot-path)
        state-atom (atom (or state initial-state))
        new-partkey (inc old-partkey)
        order-counter-atom (atom 0)
        page-size (or (:page-size aws-opts) 1000)]

    (restore-events! business-fn state-atom dynamodb-cli table old-partkey page-size)

    ; since s3 update is atomic, if saving snapshot fails next prevayler will pick the previous state
    ; and restore events from the previous partkey
    (save-snapshot! s3-cli bucket snapshot-path {:state @state-atom :partkey new-partkey})

    (reify
      Prevayler
      (handle! [this event]
        (locking this ; (I)solation: strict serializability.
          (let [timestamp (timestamp-fn)
                new-state (business-fn @state-atom event timestamp)] ; (C)onsistency: must be guaranteed by the handler. The event won't be journalled when the handler throws an exception.)
            (write-event! dynamodb-cli table new-partkey (swap! order-counter-atom inc) [timestamp event]) ; (D)urability
            (reset! state-atom new-state)))) ; (A)tomicity
      (timestamp [_] (timestamp-fn))

      IDeref (deref [_] @state-atom)

      Closeable (close [_] (aws/stop dynamodb-cli)))))
