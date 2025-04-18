import random
import logging
import time
from typing import Dict, List, Any, Tuple, Optional
from collections import deque

class LoadBalancer:
    """
    Advanced load balancer that distributes requests across multiple clients using
    a hybrid approach combining weighted randomness and least connections.
    """
    def __init__(self, clients: Dict[int, Any], work_loads: Dict[int, int]):
        """
        Initialize load balancer with clients and work_loads dicts
        
        :param clients: Dictionary mapping client IDs to client objects
        :param work_loads: Dictionary mapping client IDs to current work loads
        """
        self.clients = clients
        self.work_loads = work_loads
        self.response_times: Dict[int, deque] = {client_id: deque(maxlen=10) for client_id in clients}
        self.health_checks: Dict[int, bool] = {client_id: True for client_id in clients}
        self.last_used_time: Dict[int, float] = {client_id: 0 for client_id in clients}
        self.cooldown_period = 1.0  # seconds to wait before reusing a client
        logging.info(f"Load balancer initialized with clients: {list(clients.keys())}")
    
    def get_client(self, request_size: Optional[int] = None) -> Tuple[int, Any]:
        """
        Select the best client for the current request
        
        :param request_size: Optional size of the request in bytes
        :return: Tuple of (client_id, client_object)
        """
        # Make sure our internal dictionaries are in sync with clients
        self._ensure_client_dicts_are_in_sync()
        
        # Filter out unhealthy clients
        available_clients = {
            client_id: client 
            for client_id, client in self.clients.items() 
            if client_id in self.health_checks and self.health_checks[client_id]
        }
        
        if not available_clients:
            # If all clients are unhealthy, use any client as fallback
            logging.warning("All clients are unhealthy! Using first client as fallback.")
            client_id = next(iter(self.clients.keys()))
            return client_id, self.clients[client_id]
        
        # Strategy 1: If a client has zero load, prioritize it
        zero_load_clients = [
            client_id for client_id in available_clients 
            if client_id in self.work_loads and self.work_loads[client_id] == 0 and
            client_id in self.last_used_time and time.time() - self.last_used_time[client_id] > self.cooldown_period
        ]
        
        if zero_load_clients:
            client_id = random.choice(zero_load_clients)
            self.last_used_time[client_id] = time.time()
            return client_id, self.clients[client_id]
        
        # Strategy 2: Use weighted random selection based on:
        # - Current work load (lower is better)
        # - Average response time (lower is better)
        # - Time since last use (higher is better)
        
        scores = {}
        current_time = time.time()
        
        for client_id in available_clients:
            # Work load factor (inverse of work load)
            work_load = max(1, self.work_loads.get(client_id, 1))
            work_load_factor = 1.0 / work_load
            
            # Response time factor (use 1.0 if no data)
            if client_id in self.response_times and self.response_times[client_id]:
                avg_response_time = sum(self.response_times[client_id]) / len(self.response_times[client_id])
                response_time_factor = 1.0 / max(0.1, avg_response_time)
            else:
                response_time_factor = 1.0
            
            # Time since last use factor
            time_since_last_use = current_time - self.last_used_time.get(client_id, 0)
            time_factor = min(5.0, time_since_last_use / self.cooldown_period)
            
            # Combined score (higher is better)
            score = (work_load_factor * 0.6) + (response_time_factor * 0.2) + (time_factor * 0.2)
            scores[client_id] = max(0.1, score)  # Ensure minimum score
        
        # Choose client based on weighted probabilities
        total_score = sum(scores.values())
        weights = [scores[client_id] / total_score for client_id in available_clients]
        
        client_id = random.choices(
            list(available_clients.keys()),
            weights=weights,
            k=1
        )[0]
        
        self.last_used_time[client_id] = current_time
        return client_id, self.clients[client_id]
    
    def _ensure_client_dicts_are_in_sync(self):
        """
        Ensure all client tracking dictionaries have entries for all clients
        """
        for client_id in self.clients:
            if client_id not in self.health_checks:
                logging.info(f"Adding missing client {client_id} to health checks")
                self.health_checks[client_id] = True
            if client_id not in self.work_loads:
                self.work_loads[client_id] = 0
            if client_id not in self.response_times:
                self.response_times[client_id] = deque(maxlen=10)
            if client_id not in self.last_used_time:
                self.last_used_time[client_id] = 0
    
    def record_response_time(self, client_id: int, response_time: float) -> None:
        """
        Record response time for a client to improve future balancing decisions
        
        :param client_id: ID of the client
        :param response_time: Response time in seconds
        """
        if client_id not in self.response_times:
            self.response_times[client_id] = deque(maxlen=10)
        self.response_times[client_id].append(response_time)
    
    def mark_unhealthy(self, client_id: int) -> None:
        """
        Mark a client as unhealthy
        
        :param client_id: ID of the client to mark unhealthy
        """
        if client_id not in self.health_checks:
            self.health_checks[client_id] = False
        else:
            self.health_checks[client_id] = False
        logging.warning(f"Client {client_id} marked as unhealthy")
    
    def mark_healthy(self, client_id: int) -> None:
        """
        Mark a client as healthy
        
        :param client_id: ID of the client to mark healthy
        """
        if client_id not in self.health_checks:
            self.health_checks[client_id] = True
        else:
            self.health_checks[client_id] = True
        logging.info(f"Client {client_id} is healthy again")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of all clients
        
        :return: Dictionary with client status information
        """
        self._ensure_client_dicts_are_in_sync()
        status = {}
        for client_id in self.clients:
            avg_response_time = 0
            if client_id in self.response_times and self.response_times[client_id]:
                avg_response_time = sum(self.response_times[client_id]) / len(self.response_times[client_id])
            
            status[f"client_{client_id}"] = {
                "work_load": self.work_loads.get(client_id, 0),
                "healthy": self.health_checks.get(client_id, True),
                "avg_response_time": round(avg_response_time, 3),
                "time_since_last_use": round(time.time() - self.last_used_time.get(client_id, 0), 2)
            }
        
        return status 
