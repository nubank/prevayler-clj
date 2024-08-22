(ns prevayler-clj-aws.core-test
  (:require [prevayler-clj-aws.core :as core]
            [prevayler-clj-aws.util :as util]
            [prevayler-clj.prevayler4 :as prevayler]
            [clojure.test :refer [deftest is testing]]
            [clj-test-containers.core :as tc]
            [com.gfredericks.test.chuck.generators :as genc]
            [clojure.test.check.generators :as gen]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [meta-merge.core :refer [meta-merge]]
            [matcher-combinators.test :refer [match?]]))

(defonce localstack-port
  (memoize
   #(or (some-> (System/getenv "LOCALSTACK_PORT") (Integer/parseInt))
        (-> (tc/create {:image-name "localstack/localstack"
                        :exposed-ports [4566]})
            (tc/start!)
            :mapped-ports
            (get 4566)))))

(defn gen-name []
  (gen/generate (genc/string-from-regex #"[a-z0-9]{5,20}")))

(defn gen-opts [& {:as opts}]
  (let [s3-bucket (gen-name)
        dynamodb-table (gen-name)
        hostname (or (System/getenv "LOCALSTACK_HOST") "localhost")
        endpoint-override {:protocol "http" :hostname hostname :port (localstack-port)}
        credentials-provider (credentials/basic-credentials-provider {:access-key-id "dumb" :secret-access-key "dumb"})
        s3-cli       (aws/client {:api :s3       :endpoint-override endpoint-override :region "us-east-1" :credentials-provider credentials-provider})
        dynamodb-cli (aws/client {:api :dynamodb :endpoint-override endpoint-override :region "us-east-1" :credentials-provider credentials-provider})]
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
      (is (match? [{:Key "snapshot-v2"}] (list-objects s3-client s3-bucket)))))
  
  (testing "can override snapshot file name"
    (let [{{:keys [s3-client s3-bucket]} :aws-opts :as opts} (gen-opts :aws-opts {:snapshot-path "my-path"})
          _ (prev! opts)]
      (is (match? [{:Key "my-path-v2"}] (list-objects s3-client s3-bucket)))))
  
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
          _ (prevayler/handle! prev1 1)
          prev2 (prev! opts)
          _ (prevayler/handle! prev2 2)
          prev3 (prev! opts)]
      (is (= [1 2] @prev3))))
  
  (testing "replay more than one page"
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
      (is (= :initial-state @prev2))))
  
  (testing "snapshot! starts new journal with current state (business function is never called during start up)"
    (let [opts (gen-opts :initial-state [] :business-fn (fn [state event _] (conj state event)))
          prev1 (prev! opts)]
      (prevayler/handle! prev1 "A")
      (prevayler/handle! prev1 "B")
      (prevayler/snapshot! prev1)
      (prevayler/handle! prev1 "C")
      (prevayler/handle! prev1 "D")
      (prevayler/snapshot! prev1)
      (prevayler/snapshot! prev1)
      (let [prev2 (prev! (assoc opts :business-fn (constantly "rubbish")))]
        (is (= ["A" "B" "C" "D"] @prev2)))))

  (testing "it converts to snapshot v2"
    (let [{{:keys [s3-client s3-bucket]} :aws-opts :as opts} (gen-opts :initial-state [] :business-fn (fn [state _ _] state))
          _ (util/aws-invoke s3-client {:op :PutObject
                                        :request {:Bucket s3-bucket
                                                  :Key "snapshot"
                                                  :Body (#'core/marshal {:partkey 0
                                                                         :state :state})}})
          prev (prev! opts)] ;; saves snapshot-v2
      (is (= :state @prev))
      (util/aws-invoke s3-client {:op :DeleteObject
                                  :request {:Bucket s3-bucket
                                            :Key "snapshot"}})
      (is (= :state @(prev! opts))))))
