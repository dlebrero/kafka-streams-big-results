(ns our-service.big-file
  (:require
    [franzy.serialization.deserializers :as deserializers]
    [franzy.serialization.serializers :as serializers]
    [clojure.tools.logging :as log]
    [clojure.java.io :as io])
  (:gen-class)
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig KafkaStreams)
           (org.apache.kafka.common.serialization Serde Serdes Serializer)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams StreamsBuilder KeyValue)
           (org.apache.kafka.streams.state Stores)
           (org.apache.kafka.streams.kstream ValueTransformerWithKeySupplier ValueTransformerWithKey Predicate)))

(defmacro pred [kv & body]
  `(reify Predicate
     (test [_# ~(first kv) ~(second kv)]
       ~@body)))

(defn value-transformer-with-store [store-name f]
  (reify ValueTransformerWithKeySupplier
    (get [this]
      (let [state-store (volatile! nil)]
        (reify ValueTransformerWithKey
          (init [this context]
            (vreset! state-store (.getStateStore context store-name)))
          (transform [this key value]
            (f @state-store key value))
          (close [this]))))))

;;;
;;; Serialization stuff
;;;

(deftype NotSerializeNil [edn-serializer]
  Serializer
  (configure [_ configs isKey] (.configure edn-serializer configs isKey))
  (serialize [_ topic data]
    (when data (.serialize edn-serializer topic data)))
  (close [_] (.close edn-serializer)))

;; Can be global as they are thread-safe
(def serializer (NotSerializeNil. (serializers/edn-serializer)))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

;;;
;;; Application
;;;

(defn kafka-config []
  (doto
    (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "big-file-consumer")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/PROCESSING_GUARANTEE_CONFIG "exactly_once")
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 1000)
    (.put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(def ^String device-data-topic "file-processor")

(defn line-key [k i]
  (format "%s-part-%019d" k i))

(defn check-if-all-results-received [store file-id records-so-far total-lines]
  (when (= records-so-far total-lines)
    (log/info "Writing file " file-id)
    (with-open [all-records (.range store (line-key file-id 0) (line-key file-id Long/MAX_VALUE))
                writer (io/writer file-id)]
      (doseq [^KeyValue record (iterator-seq all-records)]
        (.write writer (str (.key record) "->" (.value record) "\n"))))
    (log/info "deleting")
    (with-open [all-records (.range store (line-key file-id 0) (line-key file-id Long/MAX_VALUE))]
      (doseq [record-batch (partition-all 1000 (iterator-seq all-records))]
        (.putAll store (mapv (fn [record] (KeyValue. (.key record) nil)) record-batch))))
    (.delete store file-id)
    {:finish file-id}))

(defn get-so-far [store file-id]
  (or (.get store file-id) {:so-far 0}))

(defn total-lines-msg-arrived [store file-id total-lines]
  (let [{:keys [so-far]} (get-so-far store file-id)]
    (.put store file-id {:total-lines total-lines :so-far so-far})
    (check-if-all-results-received store file-id so-far total-lines)))

(defn line-processed [store file-id {:keys [line-number partial-result]}]
  (let [{:keys [total-lines so-far]} (get-so-far store file-id)
        records-so-far (inc so-far)]
    (if (zero? (mod records-so-far 100))
      (log/info "so-far:" records-so-far))
    (.put store (line-key file-id line-number) partial-result)
    (.put store file-id {:total-lines total-lines :so-far records-so-far})
    (check-if-all-results-received store file-id records-so-far total-lines)))

(defn create-kafka-stream-topology-kstream []
  (let [^StreamsBuilder builder (StreamsBuilder.)
        state-name "big-file-state"
        store (Stores/keyValueStoreBuilder
                (Stores/persistentKeyValueStore state-name)
                (EdnSerde.)
                (EdnSerde.))
        _ (-> builder
            (.addStateStore store)
            (.stream device-data-topic)
            (.transformValues
              (value-transformer-with-store
                state-name
                (fn [store file-id {:keys [total-lines] :as record}]
                  (if total-lines
                    (total-lines-msg-arrived store file-id total-lines)
                    (line-processed store file-id record))))
              (into-array [state-name]))
            (.filter (pred [k v]
                       (some? v)))
            (.to "files-done"))]
    builder))

(defn start-kafka-streams []
  (let [builder (create-kafka-stream-topology-kstream)
        kafka-streams (KafkaStreams. (.build builder) (kafka-config))]
    (.start kafka-streams)
    kafka-streams))