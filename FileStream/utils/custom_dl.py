import asyncio
import logging
import time
from typing import Dict, Union, List, Set, Optional
from collections import defaultdict
from FileStream.bot import work_loads
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.types import Message

class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        
        # Session pool for connection reuse
        self.session_pool: Dict[int, List[Session]] = defaultdict(list)
        self.session_in_use: Dict[Session, bool] = {}
        self.max_sessions_per_dc = 5
        self.session_retry_count: Dict[Session, int] = defaultdict(int)
        self.max_session_retries = 3
        
        # Keep track of failed file IDs to avoid retrying constantly
        self.failed_file_ids: Set[str] = set()
        self.failure_cooldown: Dict[str, float] = {}
        self.cooldown_time = 300  # 5 minutes cooldown for failed files
        
        # Start cleanup tasks
        asyncio.create_task(self.clean_cache())
        asyncio.create_task(self.clean_sessions())
        
        # Add socket error tracking
        self.socket_errors = defaultdict(int)
        self.socket_error_threshold = 5  # Mark session as problematic after 5 errors
        self.socket_error_cooldown: Dict[Session, float] = {}
        
        # Add healthcheck task
        asyncio.create_task(self._health_check())

    async def get_file_properties(self, db_id: str, multi_clients) -> FileId:
        """
        Returns the properties of a media of a specific message in a FIleId class.
        if the properties are cached, then it'll return the cached results.
        or it'll generate the properties from the Message ID and cache them.
        """
        # Check if file ID is in failed list with cooldown
        current_time = time.time()
        if db_id in self.failed_file_ids:
            # Check if cooldown has expired
            if db_id in self.failure_cooldown and current_time < self.failure_cooldown[db_id]:
                logging.warning(f"File ID {db_id} is in cooldown period, can't access yet")
                raise ValueError(f"File ID {db_id} temporarily unavailable due to previous failures")
            else:
                # Cooldown expired, remove from failed list
                self.failed_file_ids.remove(db_id)
                if db_id in self.failure_cooldown:
                    del self.failure_cooldown[db_id]
        
        if db_id in self.cached_file_ids:
            return self.cached_file_ids[db_id]
        
        logging.debug("Before Calling generate_file_properties")
        try:
            await self.generate_file_properties(db_id, multi_clients)
            logging.debug(f"Cached file properties for file with ID {db_id}")
            return self.cached_file_ids[db_id]
        except Exception as e:
            # Mark as failed with cooldown
            self.failed_file_ids.add(db_id)
            self.failure_cooldown[db_id] = current_time + self.cooldown_time
            logging.error(f"Failed to get file properties for {db_id}: {str(e)}")
            raise
    
    async def generate_file_properties(self, db_id: str, multi_clients) -> FileId:
        """
        Generates the properties of a media file on a specific message.
        returns ths properties in a FIleId class.
        """
        logging.debug("Before calling get_file_ids")
        for attempt in range(3):  # Retry up to 3 times
            try:
                file_id = await get_file_ids(self.client, db_id, multi_clients, Message)
                logging.debug(f"Generated file ID and Unique ID for file with ID {db_id}")
                self.cached_file_ids[db_id] = file_id
                logging.debug(f"Cached media file with ID {db_id}")
                return self.cached_file_ids[db_id]
            except FloodWait as e:
                if attempt < 2:  # Don't sleep on the last attempt
                    logging.warning(f"FloodWait, sleeping for {e.x} seconds")
                    await asyncio.sleep(e.x)
                else:
                    raise  # Re-raise on final attempt
            except Exception as e:
                if attempt < 2:
                    logging.warning(f"Retrying file property generation due to error: {str(e)}")
                    await asyncio.sleep(1)
                else:
                    raise  # Re-raise on final attempt

    async def get_session_from_pool(self, client: Client, dc_id: int) -> Session:
        """
        Get a session from the pool or create a new one if none available
        """
        # Check if we have available sessions
        available_sessions = [
            session for session in self.session_pool[dc_id]
            if not self.session_in_use.get(session, False) and
            self.session_retry_count[session] < self.max_session_retries and
            session not in self.socket_error_cooldown
        ]
        
        if available_sessions:
            # Use an existing session
            session = available_sessions[0]
            self.session_in_use[session] = True
            logging.debug(f"Reusing session from pool for DC {dc_id}")
            return session
            
        # Remove sessions in cooldown that have expired
        current_time = time.time()
        for session, cooldown_time in list(self.socket_error_cooldown.items()):
            if current_time > cooldown_time:
                del self.socket_error_cooldown[session]
                logging.info(f"Session for DC {session.dc_id} released from cooldown")
            
        # Check if we've reached max sessions per DC
        if len(self.session_pool[dc_id]) >= self.max_sessions_per_dc:
            # Wait for a session to become available
            for _ in range(10):  # Wait up to 10 seconds
                await asyncio.sleep(1)
                available_sessions = [
                    session for session in self.session_pool[dc_id]
                    if not self.session_in_use.get(session, False) and
                    self.session_retry_count[session] < self.max_session_retries and
                    session not in self.socket_error_cooldown
                ]
                if available_sessions:
                    session = available_sessions[0]
                    self.session_in_use[session] = True
                    logging.debug(f"Reusing session after waiting for DC {dc_id}")
                    return session
            
            # If still no session available, reuse any session (even if in use)
            # Prioritize sessions not in cooldown
            non_cooldown_sessions = [s for s in self.session_pool[dc_id] if s not in self.socket_error_cooldown]
            if non_cooldown_sessions:
                session = non_cooldown_sessions[0]
            else:
                session = self.session_pool[dc_id][0]
                
            self.session_in_use[session] = True
            logging.warning(f"Forced reuse of busy session for DC {dc_id}")
            return session
        
        # Create a new session
        session = await self.generate_media_session(client, dc_id)
        self.session_pool[dc_id].append(session)
        self.session_in_use[session] = True
        logging.debug(f"Created new session for DC {dc_id}")
        return session

    async def release_session(self, session: Session):
        """Mark a session as no longer in use"""
        self.session_in_use[session] = False
        logging.debug(f"Released session for DC {session.dc_id}")

    async def generate_media_session(self, client: Client, dc_id: int) -> Session:
        """
        Generates the media session for the DC.
        """
        if dc_id != await client.storage.dc_id():
            media_session = Session(
                client,
                dc_id,
                await Auth(
                    client, dc_id, await client.storage.test_mode()
                ).create(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()

            for _ in range(6):
                exported_auth = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )

                try:
                    await media_session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id, bytes=exported_auth.bytes
                        )
                    )
                    break
                except AuthBytesInvalid:
                    logging.debug(
                        f"Invalid authorization bytes for DC {dc_id}"
                    )
                    continue
            else:
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            media_session = Session(
                client,
                dc_id,
                await client.storage.auth_key(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()
        
        logging.debug(f"Created media session for DC {dc_id}")
        return media_session

    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation,
                                                     raw.types.InputDocumentFileLocation,
                                                     raw.types.InputPeerPhotoFileLocation,]:
        """
        Returns the file location for the media file.
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def handle_socket_error(self, session: Session, error):
        """Handle socket errors by tracking and potentially putting sessions in cooldown"""
        self.socket_errors[session] += 1
        
        # If we've reached the error threshold, put the session in cooldown
        if self.socket_errors[session] >= self.socket_error_threshold:
            logging.warning(f"Session for DC {session.dc_id} has reached socket error threshold, putting in cooldown")
            # Put in cooldown for 5 minutes
            self.socket_error_cooldown[session] = time.time() + 300
            self.socket_errors[session] = 0  # Reset counter
            
            # Try to create a new session for this DC to replace the problematic one
            try:
                new_session = await self.generate_media_session(self.client, session.dc_id)
                self.session_pool[session.dc_id].append(new_session)
                logging.info(f"Created replacement session for DC {session.dc_id}")
            except Exception as e:
                logging.error(f"Failed to create replacement session: {str(e)}")

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[str, None]:
        """
        Custom generator that yields the bytes of the media file.
        """
        client = self.client
        work_loads[index] += 1
        media_session = None
        current_part = 0
        
        logging.debug(f"Starting to yielding file with client {index}.")
        
        try:
            # Get a session from the pool
            media_session = await self.get_session_from_pool(client, file_id.dc_id)
            
            current_part = 1
            location = await self.get_location(file_id)

            # First request with error handling and retry
            for attempt in range(3):
                try:
                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                        timeout=20  # 20 second timeout
                    )
                    break
                except (TimeoutError, ConnectionError, OSError) as e:
                    if attempt < 2:  # Don't handle on the last attempt
                        logging.warning(f"Connection error on attempt {attempt+1}: {str(e)}")
                        self.session_retry_count[media_session] += 1
                        await self.handle_socket_error(media_session, e)
                        
                        # Create new session if too many retries
                        if self.session_retry_count[media_session] >= self.max_session_retries:
                            await self.release_session(media_session)
                            media_session = await self.get_session_from_pool(client, file_id.dc_id)
                        await asyncio.sleep(1)
                    else:
                        raise  # Re-raise on final attempt

            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        break
                    elif part_count == 1:
                        yield chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        yield chunk[first_part_cut:]
                    elif current_part == part_count:
                        yield chunk[:last_part_cut]
                    else:
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    # Subsequent requests with retry logic
                    for attempt in range(3):
                        try:
                            r = await media_session.invoke(
                                raw.functions.upload.GetFile(
                                    location=location, offset=offset, limit=chunk_size
                                ),
                                timeout=20
                            )
                            break
                        except (TimeoutError, ConnectionError, OSError) as e:
                            if attempt < 2:  # Don't handle on the last attempt
                                logging.warning(f"Connection error on attempt {attempt+1}: {str(e)}")
                                await self.handle_socket_error(media_session, e)
                                await asyncio.sleep(1)
                            else:
                                logging.error(f"Failed to get chunk after retries: {str(e)}")
                                # Return what we have so far
                                return
                        
        except (TimeoutError, AttributeError, ConnectionError, OSError) as e:
            logging.error(f"Error streaming file: {str(e)}")
            if media_session:
                await self.handle_socket_error(media_session, e)
        except FloodWait as e:
            logging.warning(f"FloodWait in yield_file: {e.x} seconds")
        except asyncio.CancelledError:
            logging.info("Stream was cancelled by client")
        except Exception as e:
            logging.exception(f"Unexpected error streaming file: {str(e)}")
        finally:
            logging.debug(f"Finished yielding file with {current_part if current_part > 0 else 0} parts.")
            
            # Return session to pool
            if media_session:
                await self.release_session(media_session)
                
            work_loads[index] -= 1

    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            try:
                await asyncio.sleep(self.clean_timer)
                self.cached_file_ids.clear()
                
                # Clean failed file cooldowns
                current_time = time.time()
                expired_failures = [
                    file_id for file_id, expiry_time in self.failure_cooldown.items()
                    if current_time > expiry_time
                ]
                
                for file_id in expired_failures:
                    if file_id in self.failed_file_ids:
                        self.failed_file_ids.remove(file_id)
                    if file_id in self.failure_cooldown:
                        del self.failure_cooldown[file_id]
                
                logging.debug(f"Cleaned the cache and {len(expired_failures)} expired failure records")
            except Exception as e:
                logging.error(f"Error in cache cleaning: {str(e)}")
    
    async def clean_sessions(self) -> None:
        """
        Close and clean up sessions that haven't been used recently
        """
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                for dc_id in list(self.session_pool.keys()):
                    # Keep at least one session per DC
                    if len(self.session_pool[dc_id]) <= 1:
                        continue
                    
                    # Close sessions with high retry counts or socket errors
                    problematic_sessions = [
                        session for session in self.session_pool[dc_id]
                        if (self.session_retry_count[session] >= self.max_session_retries or
                            self.socket_errors[session] >= self.socket_error_threshold) and
                        not self.session_in_use.get(session, False)
                    ]
                    
                    for session in problematic_sessions:
                        logging.info(f"Closing problematic session for DC {dc_id}")
                        self.session_pool[dc_id].remove(session)
                        if session in self.session_in_use:
                            del self.session_in_use[session]
                        if session in self.session_retry_count:
                            del self.session_retry_count[session]
                        if session in self.socket_errors:
                            del self.socket_errors[session]
                        if session in self.socket_error_cooldown:
                            del self.socket_error_cooldown[session]
                        
                        # Close the session
                        try:
                            await session.stop()
                        except Exception as e:
                            logging.error(f"Error closing session: {str(e)}")
                
                logging.debug("Session cleanup complete")
            except Exception as e:
                logging.error(f"Error in session cleaning: {str(e)}")
                
    async def _health_check(self) -> None:
        """
        Periodically check health of sessions
        """
        while True:
            try:
                await asyncio.sleep(600)  # Check every 10 minutes
                
                # Clean up cooldown sessions
                current_time = time.time()
                for session in list(self.socket_error_cooldown.keys()):
                    if current_time > self.socket_error_cooldown[session]:
                        del self.socket_error_cooldown[session]
                        logging.info(f"Session for DC {session.dc_id} removed from cooldown")
                
                # Reset socket error counts periodically to allow recovery
                for session in list(self.socket_errors.keys()):
                    if self.socket_errors[session] > 0 and self.socket_errors[session] < self.socket_error_threshold:
                        self.socket_errors[session] = max(0, self.socket_errors[session] - 1)
                
                logging.debug("Health check complete")
            except Exception as e:
                logging.error(f"Error in health check: {str(e)}")
