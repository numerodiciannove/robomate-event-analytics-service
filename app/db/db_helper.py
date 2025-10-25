import asyncio
import time
from typing import Dict, Any, Tuple, Optional

from loguru import logger
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings


class DataBaseHelper:
    TEMP_ENGINE_TTL: int = settings.db.temp_engine_ttl
    CONNECTION_CHECK_INTERVAL: int = settings.db.connection_check_interval

    def __init__(self):
        # Default engine
        self.engine: AsyncEngine = create_async_engine(
            url=str(settings.db.url),
            echo=settings.db.echo,
            echo_pool=settings.db.echo_pool,
            pool_size=settings.db.pool_size,
            max_overflow=settings.db.max_overflow,
        )
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Stores temporary engines: {url: (engine, session_factory, last_used_timestamp)}
        self._temp_engines: Dict[str, Tuple[AsyncEngine, async_sessionmaker, float]] = {}

        # Cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        logger.info("DataBaseHelper initialized with default engine.")

    async def _start_cleanup_loop_if_needed(self):
        """Start background cleanup loop for temporary engines if not running"""
        if self._cleanup_task is None:
            loop = asyncio.get_running_loop()
            self._cleanup_task = loop.create_task(self._cleanup_loop())
            logger.info("Started cleanup loop for temporary engines.")

    async def _cleanup_loop(self):
        """Dispose expired temporary engines in background"""
        try:
            while True:
                now = time.time()
                expired = []
                for url, (engine, _, last_used) in list(self._temp_engines.items()):
                    if now - last_used > self.TEMP_ENGINE_TTL:
                        expired.append(url)

                for url in expired:
                    engine, _, _ = self._temp_engines.pop(url)
                    await engine.dispose()
                    logger.info(f"Disposed temporary engine for {url} due to TTL expiration.")

                await asyncio.sleep(self.CONNECTION_CHECK_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Cleanup loop stopped.")
            raise

    def _get_or_create_temp_engine(self, db_params: Dict[str, Any]):
        """
        Create or reuse a temporary engine.
        All important settings (echo, pool_size, etc.) come from global config.
        """
        url = db_params.get("url")
        if not url:
            raise ValueError("db_params must contain a 'url' key")

        now = time.time()

        if url in self._temp_engines:
            engine, factory, _ = self._temp_engines[url]
            self._temp_engines[url] = (engine, factory, now)
            logger.debug(f"Reusing temporary engine for {url}, updated last_used timestamp.")
        else:
            engine: AsyncEngine = create_async_engine(
                url=url,
                echo=settings.db.echo,
                echo_pool=settings.db.echo_pool,
                pool_size=settings.db.pool_size,
                max_overflow=settings.db.max_overflow,
            )
            factory = async_sessionmaker(
                bind=engine,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )
            self._temp_engines[url] = (engine, factory, now)
            logger.info(f"Created new temporary engine for {url}.")

        # Start cleanup loop if not running
        asyncio.create_task(self._start_cleanup_loop_if_needed())

        return self._temp_engines[url][0], self._temp_engines[url][1]

    async def dispose(self):
        """Dispose default engine and all temporary engines, stop cleanup"""
        await self.engine.dispose()
        logger.info("Disposed default engine.")

        for engine, _, _ in self._temp_engines.values():
            await engine.dispose()
        self._temp_engines.clear()
        logger.info("Disposed all temporary engines.")

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    def connection(self, method):
        """Decorator to automatically create session"""
        async def wrapper(*args, **kwargs):
            db_params: Optional[Dict[str, Any]] = kwargs.pop("db_params", None)

            if db_params:
                engine, temp_factory = self._get_or_create_temp_engine(db_params)
                async with temp_factory() as session:
                    try:
                        logger.debug(f"Using temporary engine for session with {db_params['url']}")
                        return await method(*args, session=session, **kwargs)
                    except Exception as e:
                        if session.in_transaction():
                            await session.rollback()
                        logger.error(f"Error in temporary session: {e}")
                        raise
            else:
                async with self.session_factory() as session:
                    try:
                        logger.debug("Using default engine for session.")
                        return await method(*args, session=session, **kwargs)
                    except Exception as e:
                        if session.in_transaction():
                            await session.rollback()
                        logger.error(f"Error in default session: {e}")
                        raise

        return wrapper


# Global helper
db_helper = DataBaseHelper()
