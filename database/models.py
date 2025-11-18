from dataclasses import dataclass
from typing import Dict, List, Optional, Set

def calculate_xp_for_level(level: int) -> int:
    """Розраховує необхідний досвід для наступного рівня"""
    if level < 1:
        return 0
    base_xp = 20  # XP для 2 рівня
    if level == 1:
        return base_xp
    
    # Коефіцієнт зменшується з 1.48 до 1.2
    min_coef = 1.2
    max_coef = 1.48
    coef = max(min_coef, max_coef - (level - 2) * 0.02)
    
    return int(calculate_xp_for_level(level - 1) * coef)

def get_level(xp: int) -> tuple[int, int, int]:
    """Повертає (поточний рівень, поточний XP, XP до наступного рівня)"""
    level = 1
    xp_needed = calculate_xp_for_level(level)
    
    while xp >= xp_needed and xp_needed > 0:
        xp -= xp_needed
        level += 1
        xp_needed = calculate_xp_for_level(level)
    
    return level, xp, xp_needed

@dataclass
class Player:
    user_id: int
    username: str
    total_xp: int = 0
    games_played: int = 0
    spy_wins: int = 0
    civilian_wins: int = 0
    banned_until: int = 0
    
    @property
    def level_info(self) -> tuple[int, int, int]:
        """Повертає (рівень, поточний XP, XP до наступного рівня)"""
        return get_level(self.total_xp)
    
    def add_xp(self, amount: int) -> bool:
        """Додає XP гравцю. Повертає True, якщо зріс рівень"""
        old_level = self.level_info[0]
        self.total_xp += amount
        return self.level_info[0] > old_level

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
