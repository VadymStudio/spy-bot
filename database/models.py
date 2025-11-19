from dataclasses import dataclass
from typing import Dict, List, Optional, Set

def calculate_xp_for_level(level: int) -> int:
    """Розраховує необхідний досвід для наступного рівня"""
    if level < 1: return 0
    if level == 1: return 20 # 20 XP для 2 рівня
    
    # Формула прогресії
    min_coef = 1.2
    max_coef = 1.48
    coef = max(min_coef, max_coef - (level - 2) * 0.02)
    
    return int(calculate_xp_for_level(level - 1) * coef)

def get_level_from_xp(total_xp: int) -> tuple[int, int, int]:
    """Повертає (рівень, поточний_xp, xp_до_наступного)"""
    level = 1
    xp_needed = calculate_xp_for_level(level)
    
    while total_xp >= xp_needed:
        total_xp -= xp_needed
        level += 1
        xp_needed = calculate_xp_for_level(level)
    
    return level, total_xp, xp_needed

@dataclass
class Player:
    user_id: int
    username: str
    total_xp: int = 0
    level: int = 1  # <--- НОВЕ ПОЛЕ
    games_played: int = 0
    spy_wins: int = 0
    civilian_wins: int = 0
    banned_until: int = 0
    
    @property
    def level_info(self):
        # Тепер ми просто беремо level з бази, але перераховуємо поточний прогрес
        _, current_xp, xp_needed = get_level_from_xp(self.total_xp)
        return self.level, current_xp, xp_needed

@dataclass
class Room:
    token: str
    admin_id: int
    location: str = None
    spy_id: int = None
    players: Dict[int, str] = None
    player_callsigns: Dict[int, str] = None
    player_roles: Dict[int, str] = None
    player_votes: Dict[int, int] = None
    votes_yes: Set[int] = None
    votes_no: Set[int] = None
    early_votes: Set[int] = None
    game_started: bool = False
    voting_started: bool = False
    voting_ended: bool = False
    spy_guessing: bool = False
    spy_guessed: bool = False
    spy_guess: str = ""
    game_ended: bool = False
    end_time: int = 0
    last_activity: int = 0

    def __post_init__(self):
        if self.players is None: self.players = {}
        if self.player_callsigns is None: self.player_callsigns = {}
        if self.player_roles is None: self.player_roles = {}
        if self.player_votes is None: self.player_votes = {}
        if self.early_votes is None: self.early_votes = set()
        if self.votes_yes is None: self.votes_yes = set()
        if self.votes_no is None: self.votes_no = set()