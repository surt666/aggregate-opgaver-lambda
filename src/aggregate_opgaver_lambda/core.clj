(ns aggregate-opgaver-lambda.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :refer [put-object]]
           ; [amazonica.aws.sns :refer [publish]]
            [amazonica.aws.dynamodbv2 :refer :all]
            [clojure.java.io :as io]
            [cheshire.core :refer :all]
            [clj-time.core :as t]
            [clj-time.format :as f])
  (:import (java.io ByteArrayInputStream)))

(defn mk-req-handler
  "Makes a request handler"
  [f & [wrt]]
  (fn [this is os context]
    (let [w (io/writer os)
             res (-> (parse-stream (io/reader is) keyword)
                  f)]
      (prn "R" res)
      ((or wrt
           (fn [res w] (.write w (prn-str res))))
        res w)
      (.flush w))))

(defn opdater-opgave [opgaver event]
  (let [id (get-in event [:payload :opgave-id])
        opgave (first (filter #(= id (:opgave-id %)) opgaver))
        opgave (cond
                 (= "opgave-sagsbehandler-tilfoejet" (:type event)) (assoc opgave :sagsbehandler (get-in event [:payload :sagsbehandler]))
                 (= "opgave-tilknyttet-sag" (:type event)) (assoc opgave :sags-id (get-in event [:payload :sags-id]))
                 (= "opgave-lukket" (:type event)) (assoc opgave :status :lukket))]
    (conj (filter #(not= id (:opgave-id %)) opgaver) opgave)))

(defn opg [opgaver event]
  (if (= (:type event) "opgave-oprettet")
    (conj opgaver (assoc (:payload event) :status "aaben" :vur-ejd-id (:vur-ejd-id event)))
    (opdater-opgave opgaver event)))

(defn specifik-opgave-aggregator [opgave events]
  (first (reduce opg (list opgave) (vec events))))

(defn generate-opgave [msg]
  (prn "M" msg)
  (let [recs (get-in msg [:Records])
        v (mapv #(Integer/parseInt (get-in % [:dynamodb :Keys :vur-ejd-id :N])) recs)
        _ (prn "V" v)
        o (map #(get-in % [:dynamodb :NewImage :payload :M :opgave-id :N]) recs)
        events (mapv #(query
                      :table-name "opgave-events"
                      :select "ALL_ATTRIBUTES"
                      :scan-index-forward true
                      :key-conditions
                      {:vur-ejd-id {:attribute-value-list [%]
                                    :comparison-operator "EQ"}}) v)
        aggrs (flatten (map #(reduce opg [] (sort-by :tx %)) (map :items events)))]
    (prn "O" o)
    (prn "E" events)
    (prn "A" aggrs)
    (map #(put-object :bucket-name "opgave-aggregates"
                      :key (str (:opgave-id %))
                      :input-stream (ByteArrayInputStream. (.getBytes (encode %)))) aggrs)))

(def -handleRequest (mk-req-handler generate-opgave))
