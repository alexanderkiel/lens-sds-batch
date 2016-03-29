(defproject lens-sds-batch "0.2"
  :description "Lens Study Data Store Batch Processor"
  :url "https://github.com/alexanderkiel/lens-sds-batch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[bidi "1.25.0"
                  :exclusions [commons-fileupload]]
                 [ch.qos.logback/logback-classic "1.1.2"]
                 [clj-time "0.11.0"]
                 [com.cognitect/transit-clj "0.8.285"]
                 [com.novemberain/langohr "3.5.1"
                  :exclusions [clj-http cheshire]]
                 [com.stuartsierra/component "0.3.0"]
                 [com.taoensso/carmine "2.12.2"]
                 [danlentz/clj-uuid "0.1.6"]
                 [environ "1.0.1"]
                 [http-kit "2.1.18"]
                 [org.clojars.akiel/lens-odm-parser "0.1"]
                 [org.clojars.akiel/shortid "0.1.2"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.371"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.reader "0.10.0"]
                 [org.slf4j/slf4j-api "1.7.7"]
                 [prismatic/plumbing "0.5.2"]
                 [prismatic/schema "1.0.4"]]

  :profiles {:dev
             {:source-paths ["dev"]
              :dependencies [[org.clojure/tools.namespace "0.2.4"]
                             [criterium "0.4.3"]
                             [juxt/iota "0.2.0"]]
              :global-vars {*print-length* 20}}

             :production
             {:main lens.core}})
