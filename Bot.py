import asyncio
import pandas as pd
from openpyxl import load_workbook
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import re
import os
from dotenv import load_dotenv
from Generator import ScheduleGenerator

load_dotenv()

TOKEN = os.getenv("TOKEN")
EXCEL_FILE = os.getenv("EXCEL_FILE", "rasp_prepare_94.xlsx")

if not TOKEN:
    raise ValueError("❌ Токен не найден! Создайте файл .env и добавьте TOKEN=your_bot_token")

#Инициализация бота
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


class ScheduleStates(StatesGroup):
    choosing_group = State()
    choosing_subgroup = State()
    editing_field = State()
    editing_value = State()
    confirm_edit = State()


class GenerateStates(StatesGroup):
    """Состояния для генерации расписания"""
    collecting_exercise_times = State()
    entering_start_time = State()
    confirm_generation = State()


#Загрузка уникальных названий групп
def load_groups():
    df_all = pd.read_excel(EXCEL_FILE, sheet_name="all", header=None)
    groups = []
    for i in range(len(df_all)):
        val = df_all.iloc[i, 1] if df_all.shape[1] > 1 else None
        if pd.notna(val) and isinstance(val, str) and "группа" in val.lower():
            groups.append(val)
    return sorted(set(groups))


def load_subgroups(group_name):
    df_prep = pd.read_excel(EXCEL_FILE, sheet_name="prep", header=None)
    subgroups = []
    for i in range(len(df_prep)):
        if df_prep.shape[1] > 2 and pd.notna(df_prep.iloc[i, 1]) and pd.notna(df_prep.iloc[i, 2]):
            group_val = str(df_prep.iloc[i, 1]).strip()
            sub_val = str(df_prep.iloc[i, 2]).strip()
            if group_name in group_val:
                subgroups.append(sub_val)
    return sorted(set(subgroups))


def get_schedule_info(group_name, subgroup_name):
    df_prep = pd.read_excel(EXCEL_FILE, sheet_name="prep", header=None)
    for idx in range(len(df_prep)):
        if df_prep.shape[1] > 9 and pd.notna(df_prep.iloc[idx, 1]) and pd.notna(
                df_prep.iloc[idx, 2]) and group_name in str(df_prep.iloc[idx, 1]).strip() and subgroup_name == str(
                df_prep.iloc[idx, 2]).strip():
            kort_letter = str(df_prep.iloc[idx, 0]) if pd.notna(df_prep.iloc[idx, 0]) else ""
            kort_map = {"k": "Корт 1", "u": "Корт 2", "d": "Корт 3", "v": "Корт 3"}
            kort = kort_map.get(kort_letter, "Не указан")

            start_time_raw = df_prep.iloc[idx, 7]
            if pd.isna(start_time_raw):
                start_time = "—"
            else:
                start_time = str(start_time_raw).split()[0] if isinstance(start_time_raw, str) else str(start_time_raw)
                start_time = re.sub(r"[^\d:]", "", start_time)
                if not start_time or start_time == "":
                    start_time = "—"

            participants = str(int(df_prep.iloc[idx, 5])) if pd.notna(df_prep.iloc[idx, 5]) and str(
                df_prep.iloc[idx, 5]).isdigit() else "—"

            poomse = []
            for col in [8, 9, 10]:
                if df_prep.shape[1] > col and pd.notna(df_prep.iloc[idx, col]):
                    val = str(df_prep.iloc[idx, col]).strip()
                    if val and val != "0" and "nan" not in val.lower():
                        poomse.append(val)
            poomse_str = ", ".join(poomse) if poomse else "—"

            return {
                "row_index": idx,
                "kort": kort,
                "start_time": start_time,
                "participants": participants,
                "poomse": poomse_str
            }
    return None


def update_excel_cell(sheet_name, row_idx, col_idx, value):
    wb = load_workbook(EXCEL_FILE)
    if sheet_name not in wb.sheetnames:
        return False
    ws = wb[sheet_name]
    ws.cell(row=row_idx + 2, column=col_idx + 1, value=value)
    wb.save(EXCEL_FILE)
    return True


@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    buttons = [
        [KeyboardButton(text="📅 Просмотреть расписание")],
        [KeyboardButton(text="🔧 Сгенерировать новое расписание")],
        [KeyboardButton(text="❌ Отмена")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("🏆 Выберите действие:", reply_markup=keyboard)
    await state.clear()


@dp.message(F.text == "📅 Просмотреть расписание")
async def view_schedule(message: types.Message, state: FSMContext):
    groups = load_groups()
    if not groups:
        await message.answer("❌ Не удалось загрузить группы из Excel.")
        return

    buttons = [[KeyboardButton(text=g)] for g in groups]
    buttons.append([KeyboardButton(text="🔙 Назад")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("🏆 Выберите группу:", reply_markup=keyboard)
    await state.set_state(ScheduleStates.choosing_group)


@dp.message(ScheduleStates.choosing_group)
async def choose_group(message: types.Message, state: FSMContext):
    if message.text in ["❌ Отмена", "🔙 Назад"]:
        await start(message, state)
        return

    groups = load_groups()
    if message.text not in groups:
        await message.answer("❌ Такой группы нет. Выберите из списка.")
        return

    await state.update_data(selected_group=message.text)
    subgroups = load_subgroups(message.text)
    if not subgroups:
        await message.answer("❌ Подгруппы не найдены.")
        return

    buttons = [[KeyboardButton(text=s)] for s in subgroups]
    buttons.append([KeyboardButton(text="🔙 Назад к группам")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer(f"Группа: *{message.text}*\nВыберите подгруппу:", reply_markup=keyboard, parse_mode="Markdown")
    await state.set_state(ScheduleStates.choosing_subgroup)


@dp.message(ScheduleStates.choosing_subgroup)
async def choose_subgroup(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await start(message, state)
        return

    if message.text == "🔙 Назад к группам":
        await view_schedule(message, state)
        return

    if message.text == "🔙 Назад":
        await start(message, state)
        return

    data = await state.get_data()
    group = data.get("selected_group")
    info = get_schedule_info(group, message.text)
    if not info:
        await message.answer("❌ Подгруппа не найдена.")
        return

    await state.update_data(current_info=info, selected_subgroup=message.text)

    text = (
        f"📋 *Расписание выступления*\n\n"
        f"🏷 Группа: `{group}`\n"
        f"🔖 Подгруппа: `{message.text}`\n"
        f"🏟 Корт: `{info['kort']}`\n"
        f"⏰ Время начала: `{info['start_time']}`\n"
        f"👥 Участников: `{info['participants']}`\n"
        f"🥋 Пхумсе: `{info['poomse']}`"
    )

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")


@dp.message(F.text == "🔙 Назад")
async def back_handler(message: types.Message, state: FSMContext):
    await start(message, state)


@dp.message(F.text == "✏️ Редактировать")
async def edit_schedule(message: types.Message, state: FSMContext):
    fields = ["⏰ Время начала", "👥 Участников", "🥋 Пхумсе", "🏟 Корт"]
    buttons = [[KeyboardButton(text=f)] for f in fields]
    buttons.append([KeyboardButton(text="❌ Отмена")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("Выберите поле для редактирования:", reply_markup=keyboard)
    await state.set_state(ScheduleStates.editing_field)


@dp.message(ScheduleStates.editing_field)
async def choose_edit_field(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await start(message, state)
        return

    field_map = {
        "⏰ Время начала": "start_time",
        "👥 Участников": "participants",
        "🥋 Пхумсе": "poomse",
        "🏟 Корт": "kort"
    }
    internal_field = field_map.get(message.text)
    if not internal_field:
        await message.answer("❌ Неверный выбор.")
        return

    await state.update_data(editing_field=internal_field)
    prompts = {
        "start_time": "Введите новое время начала (формат ЧЧ:ММ, например 10:30):",
        "participants": "Введите новое количество участников (целое число):",
        "poomse": "Введите пхумсе через запятую (например: тхэгук иль джан, кибон иль джан):",
        "kort": "Выберите корт:\n1 — Корт 1\n2 — Корт 2\n3 — Корт 3"
    }
    await message.answer(prompts[internal_field])
    await state.set_state(ScheduleStates.editing_value)


@dp.message(ScheduleStates.editing_value)
async def input_new_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    field = data["editing_field"]
    value = message.text.strip()

    if field == "start_time":
        if not re.match(r"^\d{1,2}:\d{2}$", value):
            await message.answer("❌ Неверный формат. Используйте ЧЧ:ММ")
            return
    elif field == "participants":
        if not value.isdigit() or int(value) <= 0:
            await message.answer("❌ Введите положительное целое число.")
            return
    elif field == "kort":
        if value not in ["1", "2", "3"]:
            await message.answer("❌ Введите 1, 2 или 3.")
            return

    await state.update_data(new_value=value)

    display_names = {"start_time": "время начала", "participants": "количество участников", "poomse": "пхумсе",
                     "kort": "корт"}
    confirm_text = f"Изменить *{display_names[field]}* на:\n`{value}`?"
    await message.answer(confirm_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да", callback_data="confirm_edit")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_edit")]
    ]))
    await state.set_state(ScheduleStates.confirm_edit)


@dp.callback_query(F.data == "confirm_edit")
async def confirm_edit(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    field = data["editing_field"]
    new_value = data["new_value"]
    row_idx = data["current_info"]["row_index"]

    #Преобразования значений
    if field == "kort":
        letter_map = {"1": "k", "2": "u", "3": "d"}
        excel_value = letter_map[new_value]
        col_idx = 0
    elif field == "start_time":
        excel_value = new_value
        col_idx = 7
    elif field == "participants":
        excel_value = int(new_value)
        col_idx = 5
    elif field == "poomse":
        poomse_list = [p.strip() for p in new_value.split(",") if p.strip()]
        for i, p in enumerate(poomse_list[:3]):
            update_excel_cell("prep", row_idx, 8 + i, p)
        for i in range(len(poomse_list), 3):
            update_excel_cell("prep", row_idx, 8 + i, "")
        await callback.message.edit_text("✅ Расписание успешно обновлено!")
        await start(callback.message, state)
        return

    success = update_excel_cell("prep", row_idx, col_idx, excel_value)
    if success:
        await callback.message.edit_text("✅ Расписание успешно обновлено!")
    else:
        await callback.message.edit_text("❌ Ошибка при сохранении.")

    await start(callback.message, state)


@dp.callback_query(F.data == "cancel_edit")
async def cancel_edit(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Изменения отменены.")
    await start(callback.message, state)


@dp.message(F.text == "🔧 Сгенерировать новое расписание")
async def start_generation(message: types.Message, state: FSMContext):
    await message.answer(
        "🔄 Начинаю процесс генерации расписания...\n\nСначала мне нужно узнать время выполнения каждого упражнения.")

    #Инициализация генератора
    generator = ScheduleGenerator(EXCEL_FILE)

    #Список упражнений
    exercises = generator.get_unique_exercises()

    if not exercises:
        await message.answer("❌ Не найдены упражнения в файле Excel.")
        await start(message, state)
        return

    await state.update_data(
        generator=generator,
        exercises=exercises,
        exercise_times={},
        current_exercise_index=0
    )

    await ask_exercise_time(message, state)


async def ask_exercise_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    exercises = data['exercises']
    current_index = data['current_exercise_index']

    if current_index >= len(exercises):
        #Собранные упражнения
        await ask_start_time(message, state)
        return

    current_exercise = exercises[current_index]
    await message.answer(
        f"⏱ Упражнение: *{current_exercise}*\n\n"
        f"Введите время выполнения в минутах (например: 1.5 или 2):\n"
        f"Прогресс: {current_index + 1}/{len(exercises)}",
        parse_mode="Markdown"
    )
    await state.set_state(GenerateStates.collecting_exercise_times)


@dp.message(GenerateStates.collecting_exercise_times)
async def collect_exercise_time(message: types.Message, state: FSMContext):
    #Валидация времени
    try:
        time_value = float(message.text.strip().replace(',', '.'))
        if time_value <= 0:
            await message.answer("❌ Время должно быть положительным числом. Попробуйте еще раз.")
            return
    except ValueError:
        await message.answer("❌ Неверный формат. Введите число (например: 1.5 или 2)")
        return

    data = await state.get_data()
    exercises = data['exercises']
    current_index = data['current_exercise_index']
    exercise_times = data['exercise_times']

    current_exercise = exercises[current_index]
    exercise_times[current_exercise] = time_value

    # Переходим к следующему упражнению
    await state.update_data(
        exercise_times=exercise_times,
        current_exercise_index=current_index + 1
    )

    await ask_exercise_time(message, state)


async def ask_start_time(message: types.Message, state: FSMContext):
    await message.answer(
        "✅ Все упражнения настроены!\n\n"
        "⏰ Теперь введите время начала соревнований в формате ЧЧ:ММ (например: 08:30):")
    await state.set_state(GenerateStates.entering_start_time)


@dp.message(GenerateStates.entering_start_time)
async def collect_start_time(message: types.Message, state: FSMContext):
    if not re.match(r"^\d{1,2}:\d{2}$", message.text.strip()):
        await message.answer("❌ Неверный формат. Используйте ЧЧ:ММ (например: 08:30)")
        return

    start_time = message.text.strip()
    await state.update_data(start_time=start_time)

    data = await state.get_data()
    exercise_times = data['exercise_times']

    summary = "📋 *Сводка параметров:*\n\n"
    summary += f"⏰ Время начала: `{start_time}`\n\n"
    summary += "*Время выполнения упражнений:*\n"
    for ex, time in exercise_times.items():
        summary += f"• {ex}: {time} мин\n"

    summary += "\n🔧 Сгенерировать расписание?"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, сгенерировать", callback_data="generate_schedule")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_generation")]
    ])

    await message.answer(summary, parse_mode="Markdown", reply_markup=keyboard)
    await state.set_state(GenerateStates.confirm_generation)


@dp.callback_query(F.data == "generate_schedule")
async def generate_schedule(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏳ Генерирую расписание, пожалуйста подождите...")

    try:
        data = await state.get_data()
        generator = data['generator']
        exercise_times = data['exercise_times']
        start_time = data['start_time']

        generator.set_exercise_times(exercise_times)

        schedule = generator.generate_schedule(start_time)

        if not schedule:
            await callback.message.answer("❌ Не удалось сгенерировать расписание. Проверьте данные в Excel.")
            await start(callback.message, state)
            return

        output_file = generator.save_schedule_to_excel(schedule)

        total_slots = len(schedule)
        courts = {1: 0, 2: 0, 3: 0}
        for slot in schedule:
            courts[slot.court] += 1

        end_time = max(slot.end_time for slot in schedule)

        summary = (
            f"✅ *Расписание успешно сгенерировано!*\n\n"
            f"📊 Статистика:\n"
            f"• Всего выступлений: {total_slots}\n"
            f"• Корт 1: {courts[1]} выступлений\n"
            f"• Корт 2: {courts[2]} выступлений\n"
            f"• Корт 3: {courts[3]} выступлений\n"
            f"• Начало: {start_time}\n"
            f"• Окончание: {end_time.strftime('%H:%M')}\n"
        )

        await callback.message.answer(summary, parse_mode="Markdown")

        # Отправляем расписание для каждого корта
        for court_num in [1, 2, 3]:
            court_schedule_text = generator.format_schedule_as_text(schedule, court_num)

            # Разбиваем на части, если текст слишком длинный (лимит Telegram - 4096 символов)
            max_length = 4000  # Оставляем запас
            if len(court_schedule_text) <= max_length:
                await callback.message.answer(court_schedule_text, parse_mode="Markdown")
            else:
                # Разбиваем по блокам времени
                parts = court_schedule_text.split('\n\n')
                current_part = f"*КОРТ {court_num}* (часть 1)\n" + "━" * 50 + "\n\n"
                part_num = 1

                for block in parts[1:]:  # Пропускаем заголовок
                    if len(current_part) + len(block) + 2 > max_length:
                        # Отправляем текущую часть
                        await callback.message.answer(current_part, parse_mode="Markdown")
                        await asyncio.sleep(0.3)
                        part_num += 1
                        current_part = f"*КОРТ {court_num}* (часть {part_num})\n" + "━" * 50 + "\n\n"

                    current_part += block + "\n\n"

                # Отправляем последнюю часть
                if current_part.strip():
                    await callback.message.answer(current_part, parse_mode="Markdown")

            await asyncio.sleep(0.5)  # Небольшая задержка между кортами

        # Отправляем файл
        file = FSInputFile(output_file)
        await callback.bot.send_document(callback.message.chat.id, file, caption="📄 Полное расписание в Excel")

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка при генерации: {str(e)}")

    await start(callback.message, state)


@dp.callback_query(F.data == "cancel_generation")
async def cancel_generation(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Генерация отменена.")
    await start(callback.message, state)


# Запуск
async def main():
    print("✅ Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
