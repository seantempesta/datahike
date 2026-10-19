(ns build
  (:refer-clojure :exclude [compile])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]))

(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))

(defn compile-java
  "Compile both the hand-written Java API in java/ AND the vendored
  generated API in java/src-generated/. The generated file
  (DatahikeGenerated.java) is checked in to this branch so that
  consumers using `:git/url` succeed at `deps/prep-lib` cold —
  without having to bootstrap the codegen step that needs IEntity
  pre-compiled.

  To regenerate after a spec change:
    1. clojure -T:build compile-java        ; first pass populates target/classes
    2. clojure -Scp \"$(clojure -Spath):target/classes\" -m datahike.codegen.java java/src-generated
    3. git add java/src-generated/datahike/java/DatahikeGenerated.java
    4. clojure -T:build compile-java        ; recompile with the new file"
  [_]
  (b/javac {:src-dirs ["java" "java/src-generated"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["--release" "8"
                         "-Xlint:deprecation"]}))

(defn javadoc
  "Generate Javadoc for the Java API into target/javadoc.
   tools.build has no javadoc wrapper (there is no `b/javadoc`), so shell out to the JDK
   `javadoc` tool via b/process, passing the project classpath so the Java API's imports
   (clojure.lang.*, generated classes) resolve. Output is included in the jar at release."
  [_]
  (let [out   "target/javadoc"
        cp    (str/join java.io.File/pathSeparator (:classpath-roots basis))
        srcs  (->> (io/file "java/src")
                   file-seq
                   (filter #(and (.isFile ^java.io.File %)
                                 (str/ends-with? (.getName ^java.io.File %) ".java")))
                   (mapv #(.getPath ^java.io.File %)))
        args  (into ["javadoc" "-d" out "-classpath" cp
                     "-public" "-Xdoclint:none"
                     "-windowtitle" "Datahike Java API"
                     "-doctitle" "Datahike Java API Documentation"
                     "-link" "https://docs.oracle.com/javase/8/docs/api/"
                     "-link" "https://clojure.github.io/clojure/"]
                    srcs)
        {:keys [exit]} (b/process {:command-args args})]
    ;; javadoc returns non-zero on warnings (the Java API has undocumented elements), which is
    ;; non-fatal — the HTML is still produced. Only treat it as a failure if no output appeared.
    (when-not (.exists (io/file out "index.html"))
      (throw (ex-info "javadoc produced no output" {:exit exit})))
    (when-not (zero? exit)
      (println "Note: javadoc exited" exit "(warnings above); docs generated regardless."))
    (println "Javadoc generated in" out)
    (println "Javadoc will be automatically published to javadoc.io when released to Clojars")))
