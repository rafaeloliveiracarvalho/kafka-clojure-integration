(ns kafka-clojure-project.consumer
  (:require [kafka-clojure-project.core :refer [brokers]])
  (:import
   (java.time Duration)
   (org.apache.kafka.clients.consumer KafkaConsumer)
   (org.apache.kafka.common.serialization StringDeserializer))
  (:gen-class))

(defn- create-consumer
  "Função para criação de um consumidor"
  []
  (let [props {"bootstrap.servers" brokers
               "group.id" "GrupoA"
               "enable.auto.commit" "true"
               "auto.offset.reset" "earliest"
               "key.deserializer" StringDeserializer
               "value.deserializer" StringDeserializer}]
    (KafkaConsumer. props)))

(defn read-messages
  "Função para leitura de mensagens"
  [topic]
  (with-open [consumer (create-consumer)]
    (.subscribe consumer [topic])
    (while true
      (let [records (.poll consumer (Duration/ofMillis 500))]
        (doseq [record records]
          (println (.value record)))))))

(defn -main 
  [topic]
  (read-messages topic))
