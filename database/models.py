from dataclasses import dataclass
from typing import Dict, List, Optional, Set

@dataclass
class Player:
    user_id: int
    username: str
    total_xp: int = 0
    games_played: int = 0
    spy_wins: int = 0
    civilian_wins: int = 0
    banned_until: int = 0

@dataclass
class Room:
    token: str
    admin_id: int
    location: str = None
    spy_id: int = None
    players: Dict[int, str] = None  # user_id: username
    player_roles: Dict[int, str] = None  # user_id: role ("spy" or "civilian")
    player_votes: Dict[int, int] = None  # voter_id: voted_user_id
    early_votes: Set[int] = None  # user_ids who voted to end game early
    game_started: bool = False
    voting_started: bool = False
    voting_ended: bool = False
    spy_guessing: bool = False
    spy_guessed: bool = False
    spy_guess: str = ""
    game_ended: bool = False
    end_time: int = 0  # Timestamp when game will end
    last_activity: int = 0  # Timestamp of last activity

    def __post_init__(self):
        if self.players is None:
            self.players = {}
        if self.player_roles is None:
            self.player_roles = {}
        if self.player_votes is None:
            self.player_votes = {}
        if self.early_votes is None:
            self.early_votes = set()

@dataclass
class UserState:
    current_room: str = None
    in_queue: bool = False
    last_message_time: float = 0.0
    message_count: int = 0
