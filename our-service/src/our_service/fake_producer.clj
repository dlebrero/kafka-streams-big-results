(ns our-service.fake-producer
  (:require
    [franzy.serialization.serializers :as serializers]
    [franzy.clients.producer.client :as client]
    [franzy.clients.producer.protocols :as producer]
    [compojure.core :refer [routes ANY GET POST]]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params))

(defn for-ever
  [thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (println e)
                        (Thread/sleep 100)))]
      (result 0)
      (recur))))

(def kafka-client (delay
                    (client/make-producer {:bootstrap.servers "kafka1:9092"
                                           :acks "all"
                                           :retries 1
                                           :client.id "example-producer"}
                      (serializers/string-serializer)
                      (serializers/edn-serializer))))

(defn produce-edn [m]
  (let [value (assoc-in m [:value :ts] (System/currentTimeMillis))]
    (for-ever
      #(producer/send-async! @kafka-client value))))

(defn send-command [command-key command]
  (produce-edn {:topic "file-processor"
                :key command-key
                :value command}))

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn generate-big-file [num-lines-in-file result-size]
  (let [random-order-items (shuffle (range num-lines-in-file))
        when-to-send-total (rand-int num-lines-in-file)
        file-id (str "file-" (System/currentTimeMillis))]
    (doseq [line-number random-order-items]
      (when (= line-number when-to-send-total)
        (send-command file-id {:total-lines num-lines-in-file}))
      (send-command file-id {:line-number line-number :result (rand-str result-size)}))))

(def api
  (routes
    (POST "/big-file" req
      (let [{:strs [lines size] :or {lines "100"
                                     size "1000"}} (:params req)]
        (println (:params req) "adf,dlas;f,asdlf;,ads")
        (generate-big-file (Integer/parseInt lines) (Integer/parseInt size))
        {:status 200
         :body (pr-str "done!")}))))