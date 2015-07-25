using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Threading.Tasks;
using Akka.Persistence.Journal;
using Akka.Persistence.Sql.Common;
using Akka.Persistence.Sql.Common.Journal;

namespace Akka.Persistence.Sqlite.Journal
{
    public class SqliteJournalEngine : JournalDbEngine
    {
        private readonly JournalSettings _settings;

        public SqliteJournalEngine(JournalSettings settings, Akka.Serialization.Serialization serialization) : base(settings, serialization)
        {
            _settings = settings;
            QueryBuilder = new QueryBuilder(settings);
        }

        protected override DbConnection CreateDbConnection()
        {
            return new SQLiteConnection(_settings.ConnectionString);
        }

        protected override void CopyParamsToCommand(DbCommand sqlCommand, JournalEntry entry)
        {
            sqlCommand.Parameters["@PersistenceId"].Value = entry.PersistenceId;
            sqlCommand.Parameters["@SequenceNr"].Value = entry.SequenceNr;
            sqlCommand.Parameters["@IsDeleted"].Value = entry.IsDeleted;
            sqlCommand.Parameters["@PayloadType"].Value = entry.PayloadType;
            sqlCommand.Parameters["@Payload"].Value = entry.Payload;
        }
    }

    public class SqliteJournal : AsyncWriteJournal
    {
        private readonly SqlitePersistence _extension;
        private SqliteJournalEngine _engine;

        public SqliteJournal()
        {
            _extension = SqlitePersistence.Get(Context.System);
        }

        /// <summary>
        /// Gets an engine instance responsible for handling all database-related journal requests.
        /// </summary>
        protected virtual JournalDbEngine Engine
        {
            get
            {
                return _engine ?? (_engine = new SqliteJournalEngine( _extension.JournalSettings, Context.System.Serialization));
            }
        }

        protected override void PreStart()
        {
            base.PreStart();
            Engine.Open();
        }

        protected override void PostStop()
        {
            base.PostStop();
            Engine.Close();
        }

        public override Task ReplayMessagesAsync(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> replayCallback)
        {
            return Engine.ReplayMessagesAsync(persistenceId, fromSequenceNr, toSequenceNr, max, Context.Sender, replayCallback);
        }

        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            return Engine.ReadHighestSequenceNrAsync(persistenceId, fromSequenceNr);
        }

        protected override Task WriteMessagesAsync(IEnumerable<IPersistentRepresentation> messages)
        {
            return Engine.WriteMessagesAsync(messages);
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr, bool isPermanent)
        {
            return Engine.DeleteMessagesToAsync(persistenceId, toSequenceNr, isPermanent);
        }
    }
}