"""
Database module for FTM Boss Assistant
Handles database operations for storing processed notifications
Supports both PostgreSQL and SQLite (fallback)
"""

import asyncio
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, Optional

from config import Config

logger = logging.getLogger(__name__)

# Try to import MongoDB
try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    logger.warning("pymongo not installed, MongoDB support disabled")


class Database:
    """Database handler for notification tracking"""

    def __init__(self):
        self.config = Config()
        self.db_type = None
        self.pool = None
        self.sqlite_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None

    async def initialize(self):
        """Initialize database connection and create tables"""
        logger.info("Initializing database connection...")

        # Try MongoDB first if URI is provided
        if self.config.mongodb_uri and MONGODB_AVAILABLE:
            try:
                self.mongo_client = MongoClient(self.config.mongodb_uri)
                self.mongo_db = self.mongo_client.ftm_boss_assistant
                self.mongo_collection = self.mongo_db.processed_notifications
                
                # Test connection
                self.mongo_client.admin.command('ping')
                
                # Create indexes
                self.mongo_collection.create_index("id", unique=True)
                self.mongo_collection.create_index("date")
                self.mongo_collection.create_index("processed_at")
                
                self.db_type = 'mongodb'
                logger.info("Successfully connected to MongoDB database")
                return
            except Exception as e:
                logger.warning(f"MongoDB connection failed: {e}")
                logger.info("Falling back to PostgreSQL/SQLite...")

        # Try PostgreSQL first
        try:
            import psycopg2
            import psycopg2.extras
            from psycopg2.pool import ThreadedConnectionPool

            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                **self.config.get_database_config()
            )
            self.db_type = 'postgresql'
            logger.info("Successfully connected to PostgreSQL database")

        except Exception as e:
            logger.warning(f"PostgreSQL connection failed: {e}")
            logger.info("Falling back to SQLite database...")

            # Fallback to SQLite
            try:
                self.sqlite_conn = sqlite3.connect('ftm_boss_assistant.db', check_same_thread=False)
                self.sqlite_conn.row_factory = sqlite3.Row
                self.db_type = 'sqlite'
                logger.info("Successfully connected to SQLite database")
            except Exception as sqlite_error:
                logger.error(f"Failed to initialize SQLite database: {sqlite_error}")
                raise

        # Create tables
        await self._create_tables()
        logger.info(f"Database initialized successfully using {self.db_type}")

    async def _create_tables(self):
        """Create necessary database tables"""
        if self.db_type == 'postgresql':
            await self._create_postgresql_tables()
        else:
            await self._create_sqlite_tables()

    async def _create_postgresql_tables(self):
        """Create PostgreSQL tables"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS processed_notifications (
            id VARCHAR(255) PRIMARY KEY,
            title TEXT NOT NULL,
            title_en TEXT,
            type VARCHAR(100),
            date DATE,
            pdf_url TEXT,
            notification_url TEXT,
            full_notification_data JSONB,
            telegram_message_id BIGINT,
            telegram_chat_id VARCHAR(100),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sent_to_telegram BOOLEAN DEFAULT FALSE,
            processing_status VARCHAR(50) DEFAULT 'completed',
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        await self._execute_query(create_table_sql)

        # Create indexes
        index_sql = """
        CREATE INDEX IF NOT EXISTS idx_processed_notifications_date 
        ON processed_notifications(date);

        CREATE INDEX IF NOT EXISTS idx_processed_notifications_processed_at 
        ON processed_notifications(processed_at);

        CREATE INDEX IF NOT EXISTS idx_processed_notifications_status 
        ON processed_notifications(processing_status);
        """

        await self._execute_query(index_sql)
        logger.info("PostgreSQL tables created/verified")

    async def _create_sqlite_tables(self):
        """Create SQLite tables"""
        def _create():
            cursor = self.sqlite_conn.cursor()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS processed_notifications (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                title_en TEXT,
                type TEXT,
                date TEXT,
                pdf_url TEXT,
                notification_url TEXT,
                full_notification_data TEXT,
                telegram_message_id INTEGER,
                telegram_chat_id TEXT,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                sent_to_telegram INTEGER DEFAULT 0,
                processing_status TEXT DEFAULT 'completed',
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """

            cursor.execute(create_table_sql)

            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_notifications_date 
                ON processed_notifications(date);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_notifications_processed_at 
                ON processed_notifications(processed_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_notifications_status 
                ON processed_notifications(processing_status);
            """)

            self.sqlite_conn.commit()
            logger.info("SQLite tables created/verified")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _create)

    async def _execute_query(self, query: str, params=None):
        """Execute a database query"""
        if self.db_type == 'postgresql':
            return await self._execute_postgresql_query(query, params)
        else:
            return await self._execute_sqlite_query(query, params)

    async def _execute_postgresql_query(self, query: str, params=None):
        """Execute PostgreSQL query"""
        def _execute():
            import psycopg2
            if not self.pool:
                raise Exception("Database pool not initialized")
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.fetchall() if cursor.description else None
            finally:
                self.pool.putconn(conn)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _execute)

    async def _execute_sqlite_query(self, query: str, params=None):
        """Execute SQLite query"""
        def _execute():
            # Convert PostgreSQL syntax to SQLite
            query_sqlite = query.replace('CURRENT_TIMESTAMP', "datetime('now')")
            query_sqlite = query_sqlite.replace('%s', '?')

            cursor = self.sqlite_conn.cursor()
            if params:
                cursor.execute(query_sqlite, params)
            else:
                cursor.execute(query_sqlite)
            self.sqlite_conn.commit()
            return cursor.fetchall()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _execute)

    async def _fetch_one(self, query: str, params=None):
        """Fetch one row from database"""
        if self.db_type == 'postgresql':
            return await self._fetch_one_postgresql(query, params)
        else:
            return await self._fetch_one_sqlite(query, params)

    async def _fetch_one_postgresql(self, query: str, params=None):
        """Fetch one row from PostgreSQL"""
        def _fetch():
            import psycopg2
            import psycopg2.extras
            if not self.pool:
                raise Exception("Database pool not initialized")
            conn = self.pool.getconn()
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchone()
            finally:
                self.pool.putconn(conn)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch)

    async def _fetch_one_sqlite(self, query: str, params=None):
        """Fetch one row from SQLite"""
        def _fetch():
            # Convert PostgreSQL syntax to SQLite
            query_sqlite = query.replace('%s', '?')

            cursor = self.sqlite_conn.cursor()
            if params:
                cursor.execute(query_sqlite, params)
            else:
                cursor.execute(query_sqlite)
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch)

    async def is_notification_processed(self, notification_id: str) -> bool:
        """Check if a notification has already been processed"""
        if self.db_type == 'mongodb':
            def _check():
                return self.mongo_collection.find_one({"id": notification_id}) is not None
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _check)
        
        query = "SELECT id FROM processed_notifications WHERE id = %s"
        result = await self._fetch_one(query, (notification_id,))
        return result is not None

    async def is_notification_processed_by_content(self, title: str, pdf_url: str = None, notification_url: str = None) -> bool:
        """Check if a notification has already been processed by content"""
        if self.db_type == 'mongodb':
            def _check():
                query = {
                    "title": title,
                    "$or": [
                        {"pdf_url": pdf_url},
                        {"notification_url": notification_url}
                    ]
                }
                return self.mongo_collection.find_one(query) is not None
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _check)
        
        query = """
        SELECT id FROM processed_notifications 
        WHERE title = %s 
        AND (pdf_url = %s OR notification_url = %s)
        """
        result = await self._fetch_one(query, (title, pdf_url, notification_url))
        return result is not None

    async def mark_notification_processing(self, notification: Dict):
        """Mark a notification as being processed (to prevent duplicates)"""
        if self.db_type == 'mongodb':
            def _mark():
                self.mongo_collection.update_one(
                    {"id": notification['id']},
                    {
                        "$setOnInsert": {
                            "id": notification['id'],
                            "title": notification['title'],
                            "title_en": notification.get('title_en'),
                            "type": notification.get('type'),
                            "date": notification.get('date'),
                            "pdf_url": notification.get('pdf_url'),
                            "notification_url": notification.get('url'),
                            "full_notification_data": notification,
                            "sent_to_telegram": False,
                            "processing_status": "processing",
                            "processed_at": datetime.utcnow()
                        }
                    },
                    upsert=True
                )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _mark)
            return
        
        query = """
        INSERT INTO processed_notifications 
        (id, title, title_en, type, date, pdf_url, notification_url, full_notification_data, sent_to_telegram, processing_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Parse date
        notification_date = None
        if notification.get('date'):
            try:
                notification_date = datetime.strptime(notification['date'], '%d/%m/%Y').date()
                if self.db_type == 'sqlite':
                    notification_date = notification_date.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date format for notification {notification['id']}: {notification['date']}")

        full_data = json.dumps(notification)

        params = (
            notification['id'],
            notification['title'],
            notification.get('title_en'),
            notification.get('type'),
            notification_date,
            notification.get('pdf_url'),
            notification.get('url'),
            full_data,
            0 if self.db_type == 'sqlite' else False,
            'processing'
        )

        # Handle SQLite INSERT OR IGNORE
        if self.db_type == 'sqlite':
            query = query.replace('INSERT INTO', 'INSERT OR IGNORE INTO')
        else:
            query += " ON CONFLICT (id) DO NOTHING"

        await self._execute_query(query, params)

    async def mark_notification_processed(self, notification: Dict, telegram_message_id: int = None, telegram_chat_id: str = None):
        """Mark a notification as successfully processed in the database"""
        if self.db_type == 'mongodb':
            def _mark():
                self.mongo_collection.update_one(
                    {"id": notification['id']},
                    {
                        "$set": {
                            "id": notification['id'],
                            "title": notification['title'],
                            "title_en": notification.get('title_en'),
                            "type": notification.get('type'),
                            "date": notification.get('date'),
                            "pdf_url": notification.get('pdf_url'),
                            "notification_url": notification.get('url'),
                            "full_notification_data": notification,
                            "telegram_message_id": telegram_message_id,
                            "telegram_chat_id": telegram_chat_id,
                            "sent_to_telegram": True,
                            "processing_status": "completed",
                            "processed_at": datetime.utcnow(),
                            "error_message": None
                        }
                    },
                    upsert=True
                )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _mark)
            logger.info(f"Marked notification {notification['id']} as processed with message ID {telegram_message_id}")
            return
        
        if self.db_type == 'sqlite':
            query = """
            UPDATE processed_notifications 
            SET sent_to_telegram = 1,
                processing_status = 'completed',
                processed_at = datetime('now'),
                error_message = NULL,
                telegram_message_id = %s,
                telegram_chat_id = %s
            WHERE id = %s
            """
        else:
            query = """
            UPDATE processed_notifications 
            SET sent_to_telegram = TRUE,
                processing_status = 'completed',
                processed_at = CURRENT_TIMESTAMP,
                error_message = NULL,
                telegram_message_id = %s,
                telegram_chat_id = %s
            WHERE id = %s
            """

        await self._execute_query(query, (telegram_message_id, telegram_chat_id, notification['id']))
        logger.info(f"Marked notification {notification['id']} as processed with message ID {telegram_message_id}")

    async def mark_notification_failed(self, notification: Dict, error_message: str):
        """Mark a notification as failed to prevent retry loops"""
        if self.db_type == 'mongodb':
            def _mark():
                self.mongo_collection.update_one(
                    {"id": notification['id']},
                    {
                        "$set": {
                            "processing_status": "failed",
                            "error_message": error_message[:500],
                            "processed_at": datetime.utcnow()
                        },
                        "$inc": {"retry_count": 1}
                    }
                )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _mark)
            logger.info(f"Marked notification {notification['id']} as failed")
            return
        
        if self.db_type == 'sqlite':
            query = """
            UPDATE processed_notifications 
            SET processing_status = 'failed',
                error_message = %s,
                retry_count = retry_count + 1,
                processed_at = datetime('now')
            WHERE id = %s
            """
        else:
            query = """
            UPDATE processed_notifications 
            SET processing_status = 'failed',
                error_message = %s,
                retry_count = retry_count + 1,
                processed_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """

        await self._execute_query(query, (error_message[:500], notification['id']))
        logger.info(f"Marked notification {notification['id']} as failed")

    async def get_processed_count(self) -> int:
        """Get total count of processed notifications"""
        if self.db_type == 'mongodb':
            def _count():
                return self.mongo_collection.count_documents({})
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _count)
        
        query = "SELECT COUNT(*) as count FROM processed_notifications"
        result = await self._fetch_one(query)
        return result['count'] if result else 0

    async def get_recent_notifications(self, limit: int = 10) -> list:
        """Get recent processed notifications with translations"""
        if self.db_type == 'mongodb':
            def _fetch():
                cursor = self.mongo_collection.find().sort("processed_at", -1).limit(limit)
                results = []
                for doc in cursor:
                    results.append({
                        'id': doc.get('id'),
                        'title': doc.get('title'),
                        'title_en': doc.get('title_en'),
                        'type': doc.get('type'),
                        'date': doc.get('date'),
                        'processed_at': doc.get('processed_at'),
                        'pdf_url': doc.get('pdf_url'),
                        'notification_url': doc.get('notification_url')
                    })
                return results
            
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _fetch)
        
        query = """
        SELECT id, title, title_en, type, date, processed_at, pdf_url, notification_url
        FROM processed_notifications 
        ORDER BY processed_at DESC 
        LIMIT %s
        """

        if self.db_type == 'postgresql':
            def _fetch():
                import psycopg2
                import psycopg2.extras
                if not self.pool:
                    raise Exception("Database pool not initialized")
                conn = self.pool.getconn()
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                        cursor.execute(query, (limit,))
                        return [dict(row) for row in cursor.fetchall()]
                finally:
                    self.pool.putconn(conn)

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _fetch)
        else:
            def _fetch():
                query_sqlite = query.replace('%s', '?')
                cursor = self.sqlite_conn.cursor()
                cursor.execute(query_sqlite, (limit,))
                return [dict(row) for row in cursor.fetchall()]

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _fetch)

    async def cleanup_old_records(self, days: int = 365):
        """Clean up old processed notifications (older than specified days)"""
        if self.db_type == 'mongodb':
            def _cleanup():
                cutoff_date = datetime.utcnow() - timedelta(days=days)
                result = self.mongo_collection.delete_many({"processed_at": {"$lt": cutoff_date}})
                return result.deleted_count
            
            loop = asyncio.get_event_loop()
            deleted = await loop.run_in_executor(None, _cleanup)
            logger.info(f"Cleaned up {deleted} notifications older than {days} days")
            return
        
        if self.db_type == 'sqlite':
            query = """
            DELETE FROM processed_notifications 
            WHERE datetime(processed_at) < datetime('now', '-%s days')
            """
        else:
            query = """
            DELETE FROM processed_notifications 
            WHERE processed_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
            """

        await self._execute_query(query, (days,))
        logger.info(f"Cleaned up notifications older than {days} days")

    async def close(self):
        """Close database connections"""
        if self.db_type == 'mongodb' and self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
        elif self.db_type == 'postgresql' and self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL connections closed")
        elif self.db_type == 'sqlite' and self.sqlite_conn:
            self.sqlite_conn.close()
            logger.info("SQLite connection closed")
