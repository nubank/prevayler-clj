(ns prevayler-clj-aws.util
  (:require [cognitect.aws.client.api :as aws]))

(defn aws-invoke
  [client op-map]
  (let [result (aws/invoke client op-map)]
    (if (:cognitect.anomalies/category result)
      (throw (ex-info (:message result) result))
      result)))
