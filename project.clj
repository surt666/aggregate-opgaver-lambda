(defproject aggregate-opgaver-lambda "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.amazonaws/aws-lambda-java-core "1.1.0"]
                 [expectations "2.1.9"]
                 [cheshire "5.6.3"]
                 [danlentz/clj-uuid "0.1.7"]
                 [clj-time "0.14.2"]
                 [amazonica "0.3.121" :exclusions [com.amazonaws/aws-java-sdk
                                                   com.amazonaws/amazon-kinesis-client]]
                 [com.amazonaws/aws-java-sdk-core "1.11.304"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.304"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.304"]]
  :plugins [[lein-expectations "0.0.7"]]
  :uberjar-exclusions [#".*-model\.json" #".*-intermediate\.json"]
  :aot :all)
