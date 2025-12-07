import logging
import sys
from datetime import datetime
from pathlib import Path

def setup_logging(script_path: Path) -> logging.Logger:
    """
    Configures logging to output to both console and a timestamped file.
    Also configures the root logger so that all modules can log.
    """
    # 1. Determine paths and names
    script_name = script_path.stem
    script_dir = script_path.parent
    logs_dir = script_dir / 'logs'
    
    # Create logs directory if it doesn't exist
    logs_dir.mkdir(exist_ok=True)
    
    # Generate timestamp for the log file name
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file_path = logs_dir / f"{script_name}-{timestamp}.log"

    # 2. Create formatters
    # Detailed formatter for the file
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Simpler formatter for the console
    console_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s'
    )

    # 3. Create handlers
    # File handler for writing to the log file
    file_handler = logging.FileHandler(log_file_path, 'w', encoding='utf-8')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    # Console handler for writing to the terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ ---
    # Настраиваем корневой логгер. Это заставит ВСЕ дочерние логгеры
    # (в 'utils' и т.д.) унаследовать эту конфигурацию.
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
        force=True # Перезаписывает существующую конфигурацию, если есть
    )

    # 4. Get a specific logger for the script to return it
    # Это позволяет скрипту иметь свой собственный логгер с нужным именем.
    logger = logging.getLogger(script_name)
    
    # Log an initial message indicating where the log file is saved
    logger.info(f"Logging initialized. Log file: {log_file_path}")
    
    return logger