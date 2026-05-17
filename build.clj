(ns build
  (:refer-clojure :exclude [compile])
  (:require [clojure.edn :as edn]
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

;; NOTE: the upstream `javadoc` fn used to live here. It called
;; `b/javadoc` which does not exist in any released tools.build
;; (checked 0.10.13). That call fails to LOAD this namespace under
;; the pinned tools.build, which breaks `deps/prep-lib compile-java`
;; for every consumer that pulls datahike via :git/url. Removed here
;; because it only ran at release time (to publish HTML to
;; javadoc.io). Restore once tools.build ships `javadoc`.
