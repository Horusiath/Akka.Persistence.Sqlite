using System.Data.SQLite;
using Akka.Configuration;
using Akka.Persistence.TestKit.Snapshot;
using Akka.Util.Internal;

namespace Akka.Persistence.Sqlite.Tests
{
    public class SqliteSnapshotStoreSpec : SnapshotStoreSpec
    {
        private static AtomicCounter counter = new AtomicCounter(0);
        private SQLiteConnection _conn;

        public SqliteSnapshotStoreSpec()
            : base(CreateSpecConfig("FullUri=file:memdbs-" + counter.IncrementAndGet() + ".db?mode=memory&cache=shared;"), "SqliteSnapshotStoreSpec")
        {
            SqlitePersistence.Get(Sys);

            Initialize();
        }

        private static Config CreateSpecConfig(string connectionString)
        {
            return ConfigurationFactory.ParseString(@"
                akka.persistence {
                    publish-plugin-commands = on
                    snapshot-store {
                        plugin = ""akka.persistence.snapshot-store.sqlite""
                        sqlite {
                            class = ""Akka.Persistence.Sqlite.Snapshot.SqliteSnapshotStore, Akka.Persistence.Sqlite""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            table-name = snapshot_store
                            auto-initialize = on
                            connection-string = """ + connectionString + @"""
                        }
                    }
                }");
        }
    }
}