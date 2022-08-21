(ns kafka-clojure-project.producer
  (:require [kafka-clojure-project.core :refer [brokers]])
  (:import
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
   (org.apache.kafka.common.serialization StringSerializer))
  (:gen-class))

(defn- create-producer
  "Função para criação de um produtor"
  []
  (let [props {"bootstrap.servers" brokers
               "key.serializer" StringSerializer
               "value.serializer" StringSerializer}]
    (KafkaProducer. props)))

(defn send-message
  "Função para envio de mensagens"
  [message-content topic]
  (let [producer (create-producer)
        data (ProducerRecord. topic message-content)]
    (.send producer data (println "Mensagem enviada"))
    (.close producer)))

(defn -main 
  [message topic]
  (send-message message topic))