package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.APersistentMap;
import clojure.lang.RT;
import java.util.*;

/**
 * Generated Datahike API bindings.
 * DO NOT EDIT - Generated from datahike.api.specification
 *
 * This class is package-private. Use the public Datahike facade instead.
 */
class DatahikeGenerated {

    // ===== Generated IFn Static Fields =====

    protected static final IFn asOfFn = Clojure.var("datahike.api", "as-of");
    protected static final IFn branchAsyncFn = Clojure.var("datahike.api", "branch!");
    protected static final IFn branchAsDbFn = Clojure.var("datahike.api", "branch-as-db");
    protected static final IFn branchesFn = Clojure.var("datahike.api", "branches");
    protected static final IFn commitAsDbFn = Clojure.var("datahike.api", "commit-as-db");
    protected static final IFn commitIdFn = Clojure.var("datahike.api", "commit-id");
    protected static final IFn connectFn = Clojure.var("datahike.api", "connect");
    protected static final IFn createDatabaseFn = Clojure.var("datahike.api", "create-database");
    protected static final IFn databaseExistsFn = Clojure.var("datahike.api", "database-exists?");
    protected static final IFn datomsFn = Clojure.var("datahike.api", "datoms");
    protected static final IFn dbFn = Clojure.var("datahike.api", "db");
    protected static final IFn dbWithFn = Clojure.var("datahike.api", "db-with");
    protected static final IFn deleteBranchAsyncFn = Clojure.var("datahike.api", "delete-branch!");
    protected static final IFn deleteDatabaseFn = Clojure.var("datahike.api", "delete-database");
    protected static final IFn entityFn = Clojure.var("datahike.api", "entity");
    protected static final IFn entityDbFn = Clojure.var("datahike.api", "entity-db");
    protected static final IFn explainFn = Clojure.var("datahike.api", "explain");
    protected static final IFn filterFn = Clojure.var("datahike.api", "filter");
    protected static final IFn forceBranchAsyncFn = Clojure.var("datahike.api", "force-branch!");
    protected static final IFn gcStorageFn = Clojure.var("datahike.api", "gc-storage");
    protected static final IFn historyFn = Clojure.var("datahike.api", "history");
    protected static final IFn indexRangeFn = Clojure.var("datahike.api", "index-range");
    protected static final IFn isFilteredFn = Clojure.var("datahike.api", "is-filtered");
    protected static final IFn listenFn = Clojure.var("datahike.api", "listen");
    protected static final IFn loadEntitiesFn = Clojure.var("datahike.api", "load-entities");
    protected static final IFn mergeDbFn = Clojure.var("datahike.api", "merge-db");
    protected static final IFn mergeDbAsyncFn = Clojure.var("datahike.api", "merge-db!");
    protected static final IFn metricsFn = Clojure.var("datahike.api", "metrics");
    protected static final IFn parentCommitIdsFn = Clojure.var("datahike.api", "parent-commit-ids");
    protected static final IFn pullFn = Clojure.var("datahike.api", "pull");
    protected static final IFn pullManyFn = Clojure.var("datahike.api", "pull-many");
    protected static final IFn qFn = Clojure.var("datahike.api", "q");
    protected static final IFn queryStatsFn = Clojure.var("datahike.api", "query-stats");
    protected static final IFn releaseFn = Clojure.var("datahike.api", "release");
    protected static final IFn reverseSchemaFn = Clojure.var("datahike.api", "reverse-schema");
    protected static final IFn schemaFn = Clojure.var("datahike.api", "schema");
    protected static final IFn seekDatomsFn = Clojure.var("datahike.api", "seek-datoms");
    protected static final IFn sinceFn = Clojure.var("datahike.api", "since");
    protected static final IFn tempidFn = Clojure.var("datahike.api", "tempid");
    protected static final IFn transactFn = Clojure.var("datahike.api", "transact");
    protected static final IFn transactAsyncFn = Clojure.var("datahike.api", "transact!");
    protected static final IFn unlistenFn = Clojure.var("datahike.api", "unlisten");
    protected static final IFn withFn = Clojure.var("datahike.api", "with");

    // ===== Static Initialization =====

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.api"));
    }

    // ===== Generated Static Methods =====

    /**
     * Returns database state at given time point (Date or transaction ID).
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query as of date
     * (q '[:find ?n :where [_ :name ?n]] (as-of {@literal @}conn date))
     * // Query as of transaction
     * (as-of {@literal @}conn 536870913)
     * }</pre>
     */
    public static Object asOf(Object arg0, Object arg1) {
        return (Object) asOfFn.invoke(arg0, arg1);
    }

    /**
     * Create a new branch from an existing branch or commit. Secondary indices are CoW-branched automatically.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Branch from main
     * (branch! conn :db :experiment)
     * // Branch from specific commit
     * (branch! conn #uuid "..." :hotfix)
     * }</pre>
     */
    public static Object branchAsync(Object arg0, Object arg1, Object arg2) {
        return (Object) branchAsyncFn.invoke(arg0, arg1, arg2);
    }

    /**
     * Load the database at a branch head. First argument can be a connection, db value, or raw store.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Load db at branch
     * (branch-as-db conn :experiment)
     * }</pre>
     */
    public static Object branchAsDb(Object arg0, Object arg1) {
        return (Object) branchAsDbFn.invoke(arg0, arg1);
    }

    /**
     * List all known branch names. Returns set of keywords.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // List branches
     * (branches conn)
     * }</pre>
     */
    public static Set<?> branches(Object arg0) {
        return (Set<?>) branchesFn.invoke(arg0);
    }

    /**
     * Load the database at a specific commit. First argument can be a connection, db value, or raw store.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Load db at commit
     * (commit-as-db conn #uuid "...")
     * }</pre>
     */
    public static Object commitAsDb(Object arg0, Object arg1) {
        return (Object) commitAsDbFn.invoke(arg0, arg1);
    }

    /**
     * Retrieve the commit-id for this database value.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get commit id
     * (commit-id {@literal @}conn)
     * }</pre>
     */
    public static Object commitId(Object arg0) {
        return (Object) commitIdFn.invoke(arg0);
    }

    /**
     * Connects to a Datahike database via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Connect to default in-memory database
     * (connect)
     * // Connect to file-based database
     * (connect {:store {:backend :file :path "/tmp/example"}})
     * }</pre>
     */
    public static Object connect(Map<String,Object> arg0) {
        return (Object) connectFn.invoke(Util.normalizeCollections(arg0));
    }

    /**
     * Connects to a Datahike database via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Connect to default in-memory database
     * (connect)
     * // Connect to file-based database
     * (connect {:store {:backend :file :path "/tmp/example"}})
     * }</pre>
     */
    public static Object connect(Map<String,Object> arg0, Map<?,?> arg1) {
        return (Object) connectFn.invoke(Util.normalizeCollections(arg0), Util.normalizeCollections(arg1));
    }

    /**
     * Connects to a Datahike database via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Connect to default in-memory database
     * (connect)
     * // Connect to file-based database
     * (connect {:store {:backend :file :path "/tmp/example"}})
     * }</pre>
     */
    public static Object connect() {
        return (Object) connectFn.invoke();
    }

    /**
     * Creates a database via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Create empty database
     * (create-database {:store {:backend :memory :id "example"}})
     * // Create with schema-flexibility :read
     * (create-database {:store {:backend :memory :id "example"} :schema-flexibility :read})
     * }</pre>
     */
    public static Object createDatabase(Map<String,Object> arg0) {
        return (Object) createDatabaseFn.invoke(Util.normalizeCollections(arg0));
    }

    /**
     * Creates a database via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Create empty database
     * (create-database {:store {:backend :memory :id "example"}})
     * // Create with schema-flexibility :read
     * (create-database {:store {:backend :memory :id "example"} :schema-flexibility :read})
     * }</pre>
     */
    public static Object createDatabase() {
        return (Object) createDatabaseFn.invoke();
    }

    /**
     * Checks if a database exists via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Check if in-memory database exists
     * (database-exists? {:store {:backend :memory :id "example"}})
     * // Check with default config
     * (database-exists?)
     * }</pre>
     */
    public static boolean databaseExists(Map<String,Object> arg0) {
        return (boolean) databaseExistsFn.invoke(Util.normalizeCollections(arg0));
    }

    /**
     * Checks if a database exists via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Check if in-memory database exists
     * (database-exists? {:store {:backend :memory :id "example"}})
     * // Check with default config
     * (database-exists?)
     * }</pre>
     */
    public static boolean databaseExists() {
        return (boolean) databaseExistsFn.invoke();
    }

    /**
     * Index lookup. Returns sequence of datoms matching index components.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Find all datoms for entity
     * (datoms db {:index :eavt :components [1]})
     * // Find datoms for entity and attribute
     * (datoms db {:index :eavt :components [1 :likes]})
     * }</pre>
     */
    public static Object datoms(Object arg0, Object arg1) {
        return (Object) datomsFn.invoke(arg0, arg1);
    }

    /**
     * Index lookup. Returns sequence of datoms matching index components.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Find all datoms for entity
     * (datoms db {:index :eavt :components [1]})
     * // Find datoms for entity and attribute
     * (datoms db {:index :eavt :components [1 :likes]})
     * }</pre>
     */
    public static Object datoms(Object arg0, Object arg1, Object arg2) {
        return (Object) datomsFn.invoke(arg0, arg1, arg2);
    }

    /**
     * Returns the underlying immutable database value from a connection. Prefer using {@literal @}conn directly.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get database from connection
     * (db conn)
     * // Prefer direct deref
     * {@literal @}conn
     * }</pre>
     */
    public static Object db(Object arg0) {
        return (Object) dbFn.invoke(arg0);
    }

    /**
     * Applies transaction to immutable db value, returns new db. Same as (:db-after (with db tx-data)).
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get db after transaction
     * (db-with {@literal @}conn [[:db/add 1 :name "Ivan"]])
     * }</pre>
     */
    public static Object dbWith(Object arg0, List arg1) {
        return (Object) dbWithFn.invoke(arg0, Util.normalizeCollections(arg1));
    }

    /**
     * Remove a branch. The branch data remains accessible until the next GC.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Delete branch
     * (delete-branch! conn :experiment)
     * }</pre>
     */
    public static Object deleteBranchAsync(Object arg0, Object arg1) {
        return (Object) deleteBranchAsyncFn.invoke(arg0, arg1);
    }

    /**
     * Deletes a database given via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Delete database
     * (delete-database {:store {:backend :memory :id "example"}})
     * }</pre>
     */
    public static Object deleteDatabase(Map<String,Object> arg0) {
        return (Object) deleteDatabaseFn.invoke(Util.normalizeCollections(arg0));
    }

    /**
     * Deletes a database given via configuration map.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Delete database
     * (delete-database {:store {:backend :memory :id "example"}})
     * }</pre>
     */
    public static Object deleteDatabase() {
        return (Object) deleteDatabaseFn.invoke();
    }

    /**
     * Retrieves an entity by its id. Returns lazy map-like structure.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get entity by id
     * (entity db 1)
     * // Get entity by lookup ref
     * (entity db [:email "alice{@literal @}example.com"])
     * }</pre>
     */
    public static Object entity(Object arg0, Object arg1) {
        return (Object) entityFn.invoke(arg0, arg1);
    }

    /**
     * Returns database that entity was created from.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get entity's database
     * (entity-db (entity db 1))
     * }</pre>
     */
    public static Object entityDb(Object arg0) {
        return (Object) entityDbFn.invoke(arg0);
    }

    /**
     * Returns a human-readable string explaining the query plan. Shows index selection, scan/merge ordering, recursive rule structure (SCC, base cases, clause versions), and estimated cardinalities. Takes the same arguments as `q`.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Explain a simple query
     * (explain '[:find ?e :where [?e :name]] db)
     * // Explain a recursive rule
     * (explain '[:find ?e2 :in $ ?e1 % :where (follow ?e1 ?e2)] db 1 '[[(follow ?e1 ?e2) [?e1 :follow ?e2]] [(follow ?e1 ?e2) [?e1 :follow ?t] (follow ?t ?e2)]])
     * }</pre>
     */
    public static String explain(Object arg0, Object arg1) {
        return (String) explainFn.invoke(arg0, arg1);
    }

    /**
     * Returns filtered view over database. Only includes datoms where (pred db datom) is true.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Filter to recent datoms
     * (filter db (fn [db datom] (&amp;gt; (:tx datom) recent-tx)))
     * }</pre>
     */
    public static Object filter(Object arg0, Object arg1) {
        return (Object) filterFn.invoke(arg0, arg1);
    }

    /**
     * Force a branch to point to the provided db value. WARNING: This overwrites the branch head unconditionally, like git reset --hard. Existing connections to this branch will see stale state and must be released and reconnected.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Force branch to current db
     * (force-branch! {@literal @}conn :experiment #{:db})
     * }</pre>
     */
    public static void forceBranchAsync(Object arg0, Object arg1, Set<?> arg2) {
        forceBranchAsyncFn.invoke(arg0, arg1, arg2);
    }

    /**
     * Invokes garbage collection on connection's store. Removes old snapshots before given time point.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // GC all old snapshots
     * (gc-storage conn)
     * // GC snapshots before date
     * (gc-storage conn (java.util.Date.))
     * }</pre>
     */
    public static Object gcStorage(Object arg0, Object arg1) {
        return (Object) gcStorageFn.invoke(arg0, arg1);
    }

    /**
     * Invokes garbage collection on connection's store. Removes old snapshots before given time point.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // GC all old snapshots
     * (gc-storage conn)
     * // GC snapshots before date
     * (gc-storage conn (java.util.Date.))
     * }</pre>
     */
    public static Object gcStorage(Object arg0) {
        return (Object) gcStorageFn.invoke(arg0);
    }

    /**
     * Returns full historical state of database including all assertions and retractions.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query historical data
     * (q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] (history {@literal @}conn))
     * }</pre>
     */
    public static Object history(Object arg0) {
        return (Object) historyFn.invoke(arg0);
    }

    /**
     * Returns part of :avet index between start and end values.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Find datoms in value range
     * (index-range db {:attrid :likes :start "a" :end "z"})
     * // Find entities with age in range
     * (-&amp;gt;&amp;gt; (index-range db {:attrid :age :start 18 :end 60}) (map :e))
     * }</pre>
     */
    public static Object indexRange(Object arg0, Object arg1) {
        return (Object) indexRangeFn.invoke(arg0, arg1);
    }

    /**
     * Returns true if database was filtered using filter, false otherwise.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Check if filtered
     * (is-filtered db)
     * }</pre>
     */
    public static boolean isFiltered(Object arg0) {
        return (boolean) isFilteredFn.invoke(arg0);
    }

    /**
     * Listen for changes on connection. Callback called with transaction report on each transact. WARNING: Inside the callback, use only async operations (transact!, merge-db!) — synchronous writer operations will deadlock.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Listen with callback
     * (listen conn (fn [tx-report] (println "Transaction:" (:tx-data tx-report))))
     * // Listen with key
     * (listen conn :my-listener (fn [tx-report] ...))
     * }</pre>
     */
    public static Object listen(Object arg0, Object arg1) {
        return (Object) listenFn.invoke(arg0, arg1);
    }

    /**
     * Listen for changes on connection. Callback called with transaction report on each transact. WARNING: Inside the callback, use only async operations (transact!, merge-db!) — synchronous writer operations will deadlock.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Listen with callback
     * (listen conn (fn [tx-report] (println "Transaction:" (:tx-data tx-report))))
     * // Listen with key
     * (listen conn :my-listener (fn [tx-report] ...))
     * }</pre>
     */
    public static Object listen(Object arg0, Object arg1, Object arg2) {
        return (Object) listenFn.invoke(arg0, arg1, arg2);
    }

    /**
     * Load entities directly (bulk load).
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Bulk load entities
     * (load-entities conn entities)
     * }</pre>
     */
    public static Object loadEntities(Object arg0, List arg1) {
        return (Object) loadEntitiesFn.invoke(arg0, Util.normalizeCollections(arg1));
    }

    /**
     * Create a merge commit combining the current branch with parent branches/commits. The caller provides the merged tx-data. Routed through the writer for serialization. Blocks until committed. WARNING: Do not call from listener callbacks — use merge-db! instead to avoid deadlocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Merge feature into main
     * (d/merge-db conn #{:feature} [{:name "merged entity"}])
     * // Merge with metadata
     * (d/merge-db conn #{:feature} [{:name "merged"}] {:source :merge})
     * }</pre>
     */
    public static Object mergeDb(Object arg0, Set<?> arg1, List arg2) {
        return (Object) mergeDbFn.invoke(arg0, arg1, Util.normalizeCollections(arg2));
    }

    /**
     * Create a merge commit combining the current branch with parent branches/commits. The caller provides the merged tx-data. Routed through the writer for serialization. Blocks until committed. WARNING: Do not call from listener callbacks — use merge-db! instead to avoid deadlocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Merge feature into main
     * (d/merge-db conn #{:feature} [{:name "merged entity"}])
     * // Merge with metadata
     * (d/merge-db conn #{:feature} [{:name "merged"}] {:source :merge})
     * }</pre>
     */
    public static Object mergeDb(Object arg0, Set<?> arg1, List arg2, Object arg3) {
        return (Object) mergeDbFn.invoke(arg0, arg1, Util.normalizeCollections(arg2), arg3);
    }

    /**
     * Async version of merge-db. Returns a promise (CLJ) or channel (CLJS). Safe to call from listener callbacks and go blocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Async merge
     * {@literal @}(d/merge-db! conn #{:feature} [{:name "merged"}])
     * }</pre>
     */
    public static Object mergeDbAsync(Object arg0, Set<?> arg1, List arg2) {
        return (Object) mergeDbAsyncFn.invoke(arg0, arg1, Util.normalizeCollections(arg2));
    }

    /**
     * Async version of merge-db. Returns a promise (CLJ) or channel (CLJS). Safe to call from listener callbacks and go blocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Async merge
     * {@literal @}(d/merge-db! conn #{:feature} [{:name "merged"}])
     * }</pre>
     */
    public static Object mergeDbAsync(Object arg0, Set<?> arg1, List arg2, Object arg3) {
        return (Object) mergeDbAsyncFn.invoke(arg0, arg1, Util.normalizeCollections(arg2), arg3);
    }

    /**
     * Returns database metrics (datom counts, index sizes, etc).
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get metrics
     * (metrics {@literal @}conn)
     * }</pre>
     */
    public static Object metrics(Object arg0) {
        return (Object) metricsFn.invoke(arg0);
    }

    /**
     * Retrieve parent commit ids from this database value.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get parent commits
     * (parent-commit-ids {@literal @}conn)
     * }</pre>
     */
    public static Set<?> parentCommitIds(Object arg0) {
        return (Set<?>) parentCommitIdsFn.invoke(arg0);
    }

    /**
     * Fetches data using recursive declarative pull pattern.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Pull with pattern
     * (pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)
     * // Pull with arg-map
     * (pull db {:selector [:db/id :name] :eid 1})
     * }</pre>
     */
    public static Map<?,?> pull(Object arg0, Object arg1) {
        return (Map<?,?>) pullFn.invoke(arg0, arg1);
    }

    /**
     * Fetches data using recursive declarative pull pattern.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Pull with pattern
     * (pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)
     * // Pull with arg-map
     * (pull db {:selector [:db/id :name] :eid 1})
     * }</pre>
     */
    public static Map<?,?> pull(Object arg0, List<?> arg1, Object arg2) {
        return (Map<?,?>) pullFn.invoke(arg0, Util.normalizeCollections(arg1), arg2);
    }

    /**
     * Same as pull, but accepts sequence of ids and returns sequence of maps.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Pull multiple entities
     * (pull-many db [:db/id :name] [1 2 3])
     * }</pre>
     */
    public static Iterable<?> pullMany(Object arg0, Object arg1) {
        return (Iterable<?>) pullManyFn.invoke(arg0, arg1);
    }

    /**
     * Same as pull, but accepts sequence of ids and returns sequence of maps.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Pull multiple entities
     * (pull-many db [:db/id :name] [1 2 3])
     * }</pre>
     */
    public static Iterable<?> pullMany(Object arg0, List<?> arg1, Object arg2) {
        return (Iterable<?>) pullManyFn.invoke(arg0, Util.normalizeCollections(arg1), arg2);
    }

    /**
     * Executes a datalog query.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query with vector syntax
     * (q '[:find ?value :where [_ :likes ?value]] db)
     * // Query with map syntax
     * (q '{:find [?value] :where [[_ :likes ?value]]} db)
     * }</pre>
     */
    public static Object q(Object arg0) {
        return (Object) qFn.invoke(arg0);
    }

    /**
     * Executes a datalog query.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query with vector syntax
     * (q '[:find ?value :where [_ :likes ?value]] db)
     * // Query with map syntax
     * (q '{:find [?value] :where [[_ :likes ?value]]} db)
     * }</pre>
     */
    public static Object q(Object arg0, Object arg1) {
        return (Object) qFn.invoke(arg0, arg1);
    }

    /**
     * Executes query and returns execution statistics.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query with stats
     * (query-stats '[:find ?e :where [?e :name]] db)
     * }</pre>
     */
    public static Map<?,?> queryStats(Object arg0) {
        return (Map<?,?>) queryStatsFn.invoke(arg0);
    }

    /**
     * Executes query and returns execution statistics.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query with stats
     * (query-stats '[:find ?e :where [?e :name]] db)
     * }</pre>
     */
    public static Map<?,?> queryStats(Object arg0, Object arg1) {
        return (Map<?,?>) queryStatsFn.invoke(arg0, arg1);
    }

    /**
     * Releases a database connection.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Release connection
     * (release conn)
     * }</pre>
     */
    public static void release(Object arg0) {
        releaseFn.invoke(arg0);
    }

    /**
     * Returns reverse schema definition (attribute id to ident mapping).
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get reverse schema
     * (reverse-schema {@literal @}conn)
     * }</pre>
     */
    public static Map<?,?> reverseSchema(Object arg0) {
        return (Map<?,?>) reverseSchemaFn.invoke(arg0);
    }

    /**
     * Returns current schema definition.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Get schema
     * (schema {@literal @}conn)
     * }</pre>
     */
    public static Object schema(Object arg0) {
        return (Object) schemaFn.invoke(arg0);
    }

    /**
     * Like datoms, but returns datoms starting from specified components through end of index.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Seek from entity
     * (seek-datoms db {:index :eavt :components [1]})
     * }</pre>
     */
    public static Object seekDatoms(Object arg0, Object arg1) {
        return (Object) seekDatomsFn.invoke(arg0, arg1);
    }

    /**
     * Like datoms, but returns datoms starting from specified components through end of index.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Seek from entity
     * (seek-datoms db {:index :eavt :components [1]})
     * }</pre>
     */
    public static Object seekDatoms(Object arg0, Object arg1, Object arg2) {
        return (Object) seekDatomsFn.invoke(arg0, arg1, arg2);
    }

    /**
     * Returns database state since given time point (Date or transaction ID). Contains only datoms added since that point.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Query since date
     * (since {@literal @}conn (java.util.Date.))
     * // Query since transaction
     * (since {@literal @}conn 536870913)
     * }</pre>
     */
    public static Object since(Object arg0, Object arg1) {
        return (Object) sinceFn.invoke(arg0, arg1);
    }

    /**
     * Allocates temporary id (negative integer). Prefer using negative integers directly.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Generate tempid
     * (tempid :db.part/user)
     * // Prefer direct negative integers
     * (transact conn [{:db/id -1 :name "Alice"}])
     * }</pre>
     */
    public static Object tempid(Object arg0) {
        return (Object) tempidFn.invoke(arg0);
    }

    /**
     * Allocates temporary id (negative integer). Prefer using negative integers directly.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Generate tempid
     * (tempid :db.part/user)
     * // Prefer direct negative integers
     * (transact conn [{:db/id -1 :name "Alice"}])
     * }</pre>
     */
    public static Object tempid(Object arg0, int arg1) {
        return (Object) tempidFn.invoke(arg0, arg1);
    }

    /**
     * Applies transaction to the database and updates connection. Blocks until committed. WARNING: Do not call from listener callbacks or transaction functions — use transact! instead to avoid deadlocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Add single datom
     * (transact conn [[:db/add 1 :name "Ivan"]])
     * // Retract datom
     * (transact conn [[:db/retract 1 :name "Ivan"]])
     * }</pre>
     */
    public static Object transact(Object arg0, List arg1) {
        return (Object) transactFn.invoke(arg0, Util.normalizeCollections(arg1));
    }

    /**
     * Same as transact, but asynchronously returns a future. Safe to call from listener callbacks and go blocks.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Async transaction
     * {@literal @}(transact! conn [{:db/id -1 :name "Alice"}])
     * }</pre>
     */
    public static Object transactAsync(Object arg0, List arg1) {
        return (Object) transactAsyncFn.invoke(arg0, Util.normalizeCollections(arg1));
    }

    /**
     * Removes registered listener from connection.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Remove listener
     * (unlisten conn :my-listener)
     * }</pre>
     */
    public static Map<?,?> unlisten(Object arg0, Object arg1) {
        return (Map<?,?>) unlistenFn.invoke(arg0, arg1);
    }

    /**
     * Applies transaction to immutable db value. Returns transaction report.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Transaction on db value
     * (with {@literal @}conn [[:db/add 1 :name "Ivan"]])
     * // With metadata
     * (with {@literal @}conn {:tx-data [...] :tx-meta {:source :import}})
     * }</pre>
     */
    public static Object with(Object arg0, Object arg1) {
        return (Object) withFn.invoke(arg0, arg1);
    }

    /**
     * Applies transaction to immutable db value. Returns transaction report.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Transaction on db value
     * (with {@literal @}conn [[:db/add 1 :name "Ivan"]])
     * // With metadata
     * (with {@literal @}conn {:tx-data [...] :tx-meta {:source :import}})
     * }</pre>
     */
    public static Object with(Object arg0, List arg1) {
        return (Object) withFn.invoke(arg0, Util.normalizeCollections(arg1));
    }

    /**
     * Applies transaction to immutable db value. Returns transaction report.
     * 
     * <h3>Examples:</h3>
     * <pre>{@code
     * // Transaction on db value
     * (with {@literal @}conn [[:db/add 1 :name "Ivan"]])
     * // With metadata
     * (with {@literal @}conn {:tx-data [...] :tx-meta {:source :import}})
     * }</pre>
     */
    public static Object with(Object arg0, List arg1, Object arg2) {
        return (Object) withFn.invoke(arg0, Util.normalizeCollections(arg1), arg2);
    }
}
