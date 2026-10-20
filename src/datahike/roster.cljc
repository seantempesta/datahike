(ns datahike.roster
  "The branch roster: which branches a store has, as a konserve append log under
  :branches. One entry per branch! or delete-branch!, so both are O(1) whatever the
  number of branches (the roster used to be one set rewritten on every change)."
  (:require [konserve.core :as k]))

(def roster-key
  "The store key of the branch roster: a konserve append log of [:+ branch] and
  [:- branch] entries. A branch! or delete-branch! appends one entry, O(1) whatever
  the number of branches; the roster is the log folded into a set."
  :branches)

(defn roster-add!
  "Append `branch` to the roster. Callers append only a branch whose head they just
  created (its key did not exist), so the log holds one [:+] per live branch."
  ([store branch] (roster-add! store branch {:sync? true}))
  ([store branch opts] (k/append store roster-key [:+ branch] (select-keys opts [:sync?]))))

(defn roster-remove!
  "Append the removal of `branch` to the roster."
  ([store branch] (roster-remove! store branch {:sync? true}))
  ([store branch opts] (k/append store roster-key [:- branch] (select-keys opts [:sync?]))))

(defn roster
  "The set of branch names: the roster log folded in order."
  ([store] (roster store {:sync? true}))
  ([store opts]
   (k/reduce-log store roster-key
                 (fn [acc [op branch]] (if (= op :+) (conj acc branch) (disj acc branch)))
                 #{} (select-keys opts [:sync?]))))

(defn roster-store-keys
  "Every store key the roster log occupies (its head and each element): what a
  collector keeps so the roster survives a sweep."
  [store]
  (let [[_ _ first-id] (k/get store roster-key nil {:sync? true})]
    (loop [id first-id acc #{roster-key}]
      (if id
        (recur (:next (k/get store id nil {:sync? true})) (conj acc id))
        acc))))
