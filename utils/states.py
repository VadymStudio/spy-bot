from aiogram.fsm.state import State, StatesGroup

class PlayerState(StatesGroup):
    """Стани гравця під час гри"""
    in_queue = State()
    waiting_for_token = State()
    in_lobby = State()
    in_game = State()
    voting = State()
    spy_guessing = State()

class AdminState(StatesGroup):
    """Стани адміністратора"""
    waiting_for_db_file = State()
    waiting_for_ban_user = State()
    waiting_for_unban_user = State()
    waiting_for_whois = State()

class GameState(StatesGroup):
    """Загальні стани гри"""
    maintenance = State()
    normal = State()

class RoomState(StatesGroup):
    """Стани кімнати"""
    waiting_for_players = State()
    game_in_progress = State()
    voting_in_progress = State()
    spy_guessing = State()
    game_ended = State()

# Додаткові стани для різних етапів гри
class VotingState(StatesGroup):
    """Стани процесу голосування"""
    waiting_for_votes = State()
    voting_completed = State()

class SpyGuessingState(StatesGroup):
    """Стани процесу вгадування шпигуном"""
    waiting_for_guess = State()
    guess_received = State()
