using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Text;
using Akka.Persistence.Sql.Common;
using Akka.Persistence.Sql.Common.Journal;

namespace Akka.Persistence.Sqlite.Journal
{
    internal class QueryBuilder : IJournalQueryBuilder
    {
        private readonly JournalSettings _settings;

        private readonly string _selectHighestSequenceNrSql;
        private readonly string _insertMessagesSql;

        public QueryBuilder(JournalSettings settings)
        {
            _settings = settings;

            _insertMessagesSql = string.Format(
                "INSERT INTO {0} (persistence_id, sequence_nr, is_deleted, payload_type, payload) VALUES (@PersistenceId, @SequenceNr, @IsDeleted, @PayloadType, @Payload)", _settings.TableName);
            _selectHighestSequenceNrSql = string.Format(@"SELECT MAX(sequence_nr) FROM {0} WHERE persistence_id = ? ", _settings.TableName);
        }

        public DbCommand SelectMessages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max)
        {
            var sb = new StringBuilder(@"
                SELECT persistence_id, sequence_nr, is_deleted, payload_type, payload FROM ").Append(_settings.TableName)
                .Append(" WHERE persistence_id = ? ");

            if (fromSequenceNr > 0)
            {
                if (toSequenceNr != long.MaxValue)
                    sb.Append(" AND sequence_nr BETWEEN ")
                        .Append(fromSequenceNr)
                        .Append(" AND ")
                        .Append(toSequenceNr);
                else
                    sb.Append(" AND sequence_nr >= ").Append(fromSequenceNr);
            }

            if (toSequenceNr != long.MaxValue)
                sb.Append(" AND sequence_nr <= ").Append(toSequenceNr);

            if (max != long.MaxValue)
            {
                sb.Append(" LIMIT ").Append(max);
            }

            var command = new SQLiteCommand(sb.ToString())
            {
                Parameters = { new SQLiteParameter { Value = persistenceId } }
            };

            return command;
        }

        public DbCommand SelectHighestSequenceNr(string persistenceId)
        {
            return new SQLiteCommand(_selectHighestSequenceNrSql)
            {
                Parameters = { new SQLiteParameter { Value = persistenceId } }
            };
        }

        public DbCommand InsertBatchMessages(IPersistentRepresentation[] messages)
        {
            var command = new SQLiteCommand(_insertMessagesSql);
            command.Parameters.Add("@PersistenceId", DbType.String);
            command.Parameters.Add("@SequenceNr", DbType.Int64);
            command.Parameters.Add("@IsDeleted", DbType.Boolean);
            command.Parameters.Add("@PayloadType", DbType.String);
            command.Parameters.Add("@Payload", DbType.Binary);

            return command;
        }

        public DbCommand DeleteBatchMessages(string persistenceId, long toSequenceNr, bool permanent)
        {
            var sb = new StringBuilder();

            if (permanent)
            {
                sb.Append("DELETE FROM ").Append(_settings.TableName);
            }
            else
            {
                sb.AppendFormat("UPDATE {0} SET is_deleted = 1", _settings.TableName);    
            }

            sb.Append(" WHERE persistence_id = ?");

            if (toSequenceNr != long.MaxValue)
            {
                sb.Append(" AND sequence_nr <= ").Append(toSequenceNr);
            }

            return new SQLiteCommand(sb.ToString())
            {
                Parameters = { new SQLiteParameter { Value = persistenceId } }
            };
        }
    }
}