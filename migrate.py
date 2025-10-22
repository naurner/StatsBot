import shutil
from pathlib import Path
from datetime import datetime
import json


def migrate_data():
    """Мигрирует данные из старого формата в новый."""
    print("🔄 Начало миграции данных...\n")

    base_dir = Path(__file__).parent
    data_dir = base_dir / 'data'

    # Создаем директорию data, если её нет
    data_dir.mkdir(exist_ok=True)
    print(f"✅ Создана директория: {data_dir}")

    # 1. Перенос Excel файла
    old_excel = base_dir / "Nicole_earn_data.xlsx"
    new_excel = data_dir / "model.xlsx"

    if old_excel.exists() and not new_excel.exists():
        shutil.copy2(old_excel, new_excel)
        print(f"✅ Скопирован файл: {old_excel.name} → {new_excel.name}")
    elif new_excel.exists():
        print(f"⚠️  Файл {new_excel.name} уже существует, пропускаем")
    else:
        print(f"ℹ️  Файл {old_excel.name} не найден")

    # 2. Миграция дат из текстовых файлов в JSON
    dates_history_file = base_dir / 'dates_history.txt'
    last_date_file = base_dir / 'last_date.txt'
    config_file = data_dir / 'config.json'

    config = {
        'dates_history': [],
        'last_date': None,
        'settings': {
            'earnings_percentage': 0.15
        }
    }

    # Читаем историю дат
    if dates_history_file.exists():
        try:
            with open(dates_history_file, 'r', encoding='utf-8') as f:
                lines = [ln.strip() for ln in f if ln.strip()]

            for line in lines:
                parts = line.split()
                if len(parts) == 2:
                    try:
                        dt = datetime.strptime(parts[0], '%Y-%m-%d')
                        dp = datetime.strptime(parts[1], '%Y-%m-%d')
                        config['dates_history'].append({
                            'date_till': dt.isoformat(),
                            'date_of_pay': dp.isoformat()
                        })
                    except Exception as e:
                        print(f"⚠️  Ошибка при парсинге строки: {line} - {e}")

            if config['dates_history']:
                print(f"✅ Мигрировано {len(config['dates_history'])} записей из dates_history.txt")
        except Exception as e:
            print(f"⚠️  Ошибка при чтении {dates_history_file}: {e}")

    # Читаем последнюю дату
    if last_date_file.exists():
        try:
            with open(last_date_file, 'r', encoding='utf-8') as f:
                last_date_str = f.read().strip()

            if last_date_str:
                try:
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
                    config['last_date'] = last_date.isoformat()
                    print(f"✅ Мигрирована последняя дата: {last_date_str}")
                except Exception:
                    # Пробуем другой формат
                    try:
                        last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                        config['last_date'] = last_date.isoformat()
                        print(f"✅ Мигрирована последняя дата: {last_date_str}")
                    except Exception as e:
                        print(f"⚠️  Не удалось распарсить дату: {last_date_str} - {e}")
        except Exception as e:
            print(f"⚠️  Ошибка при чтении {last_date_file}: {e}")

    # Сохраняем конфигурацию
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2, default=str)
        print(f"✅ Конфигурация сохранена в {config_file.name}")
    except Exception as e:
        print(f"❌ Ошибка при сохранении конфигурации: {e}")

    print("\n" + "=" * 60)
    print("🎉 Миграция завершена!")
    print("=" * 60)
    print("\nℹ️  Старые файлы сохранены и могут быть удалены после проверки:")
    print(f"   - {old_excel}")
    print(f"   - {dates_history_file}")
    print(f"   - {last_date_file}")
    print("\nℹ️  Новые файлы:")
    print(f"   - {new_excel}")
    print(f"   - {config_file}")


if __name__ == "__main__":
    migrate_data()