(ns prevayler-clj-aws.core-test
  (:require [prevayler-clj-aws.core :as core]
            [prevayler-clj-aws.util :as util]
            [prevayler-clj.prevayler4 :as prevayler]
            [clojure.test :refer [deftest is testing]]
            [clj-test-containers.core :as tc]
            [com.gfredericks.test.chuck.generators :as genc]
            [clojure.test.check.generators :as gen]
            [cognitect.aws.client.api :as aws]
            [meta-merge.core :refer [meta-merge]]
            [matcher-combinators.test :refer [match?]]))

(defonce aws-endpoint
  (memoize
   #(let [{:keys [mapped-ports]} (-> (tc/create {:image-name "localstack/localstack"
                                                 :exposed-ports [4566]
                                                 :env-vars {"SERVICES" "dynamodb,s3"}})
                                     (tc/start!))]
      {:protocol "http"
       :hostname "localhost"
       :port (get mapped-ports 4566)})))

(defn gen-name []
  (gen/generate (genc/string-from-regex #"[a-z0-9]{5,20}")))

(defn gen-opts [& {:as opts}]
  (let [s3-bucket (gen-name)
        dynamodb-table (gen-name)
        s3-cli (aws/client {:api :s3 :endpoint-override (aws-endpoint)})
        dynamodb-cli (aws/client {:api :dynamodb :endpoint-override (aws-endpoint)})]
    (util/aws-invoke s3-cli {:op :CreateBucket :request {:Bucket s3-bucket}})
    (util/aws-invoke dynamodb-cli {:op :CreateTable :request {:TableName dynamodb-table
                                                              :AttributeDefinitions [{:AttributeName "partkey"
                                                                                      :AttributeType "N"}
                                                                                     {:AttributeName "order"
                                                                                      :AttributeType "N"}]
                                                              :KeySchema [{:AttributeName "partkey"
                                                                           :KeyType "HASH"}
                                                                          {:AttributeName "order"
                                                                           :KeyType "RANGE"}]
                                                              :BillingMode "PAY_PER_REQUEST"}})
    (meta-merge {:aws-opts {:s3-bucket s3-bucket
                            :dynamodb-table dynamodb-table
                            :s3-client s3-cli
                            :dynamodb-client dynamodb-cli}}
                opts)))

(defn prev!
  [opts]
  (core/prevayler! (meta-merge {:business-fn (fn [state _ _] state)} opts)))

(defn list-objects [s3-client bucket]
  (-> (util/aws-invoke s3-client {:op :ListObjects
                                  :request {:Bucket bucket}})
      :Contents))

(deftest prevayler!-test
  (testing "default timestamp-fn is system clock"
    (let [prevayler (prev! (gen-opts))
          t0 (- (System/currentTimeMillis) 1)]
      (is (> (prevayler/timestamp prevayler) t0))))
  (testing "can override timestamp-fn"
    (let [prevayler (prev! (gen-opts :timestamp-fn (constantly :timestamp)))]
      (is (= :timestamp
             (prevayler/timestamp prevayler)))))
  (testing "snapshot is the default snapshot file name"
    (let [{{:keys [s3-client s3-bucket]} :aws-opts :as opts} (gen-opts)
          _ (prev! opts)]
      (is (match? [{:Key "snapshot"}] (list-objects s3-client s3-bucket)))))
  (testing "can override snapshot file name"
    (let [{{:keys [s3-client s3-bucket]} :aws-opts :as opts} (gen-opts :aws-opts {:snapshot-path "my-path"})
          _ (prev! opts)]
      (is (match? [{:Key "my-path"}] (list-objects s3-client s3-bucket)))))
  (testing "default initial state is empty map"
    (let [prevayler (prev! (gen-opts))]
      (is (= {} @prevayler))))
  (testing "can override initial state"
    (let [prevayler (prev! (gen-opts :initial-state :initial-state))]
      (is (= :initial-state
             @prevayler))))
  (testing "handles event"
    (let [prevayler (prev! (gen-opts :initial-state []
                                     :business-fn (fn [state event timestamp]
                                                    (conj state [event timestamp]))
                                     :timestamp-fn (constantly :timestamp)))
          _ (prevayler/handle! prevayler :event)]
      (is (= [[:event :timestamp]]
             @prevayler))))
  (testing "restart after some events recover last state"
    (let [opts (gen-opts :initial-state [] :business-fn (fn [state event _] (conj state event)))
          prev1 (prev! opts)
          _ (prevayler/handle! prev1 1)
          _ (prevayler/handle! prev1 2)
          prev2 (prev! opts)]
      (is (= [1 2] @prev2))))
  (testing "only replay events since last restart"
    (let [opts (gen-opts :initial-state [] :business-fn (fn [state event _] (conj state event)))
          prev1 (prev! opts)
          _ (prevayler/handle! prev1 :previous-event)
          prev2 (prev! opts)
          _ (prevayler/handle! prev2 :latest-event)
          prev3 (prev! (assoc opts :business-fn (fn [state event _] {:state state :event event})))]
      (is (= {:state [:previous-event] :event :latest-event} @prev3))))
  (testing "replay all events since last restart"
    (let [opts (gen-opts :initial-state []
                         :business-fn (fn [state event _] (conj state event))
                         :aws-opts {:page-size 1})
          prev1 (prev! opts)
          _ (prevayler/handle! prev1 1)
          _ (prevayler/handle! prev1 2)
          prev2 (prev! opts)]
      (is (= [1 2] @prev2))))
  (testing "exception in event handler does not affect state"
    (let [opts (gen-opts :initial-state :initial-state :business-fn (fn [_ _ _]
                                                                      (throw (ex-info "boom" {}))))
          prev1 (prev! opts)
          _ (try
              (prevayler/handle! prev1 :event)
              (catch Exception _))
          prev2 (prev! opts)]
      (is (= :initial-state @prev1))
      (is (= :initial-state @prev2)))))
