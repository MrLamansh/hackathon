import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass


@dataclass
class Stage:
    group_name: str
    subgroup_name: str
    stage_type: str
    participants: int
    duration_minutes: float
    exercises: List[str]
    stage_order: int
    group_id: str


@dataclass
class ScheduleSlot:
    court: int  # 1, 2, 3
    start_time: datetime
    end_time: datetime
    stage: Stage


class ScheduleGenerator:
    BREAK_BETWEEN_GROUPS = 2  # минуты
    LUNCH_START = 13 * 60  # 13:00 в минутах
    LUNCH_DURATION = 30  # минут
    LUNCH_TOLERANCE = 30  # ±30 минут от 13:00

    def __init__(self, processed_data_file: str):
        self.processed_data_file = processed_data_file
        self.exercise_times: Dict[str, float] = {}

    def get_unique_exercises(self) -> List[str]:
        df = pd.read_excel(self.processed_data_file, header=None)

        exercises = set()
        # Проходим по столбцам 3, 4, 5 (отбор, полуф, финал)
        for col in [3, 4, 5]:
            if df.shape[1] > col:
                for val in df.iloc[:, col]:
                    if pd.notna(val) and isinstance(val, str):
                        val = val.strip()
                        if val and val.lower() != 'nan':
                            exercises.add(val)

        return sorted(list(exercises))

    def set_exercise_times(self, exercise_times: Dict[str, float]):
        self.exercise_times = exercise_times

    def calculate_stage_duration(self, participants: int, exercise_time: float) -> float:
        duration = participants * exercise_time

        if participants > 19:
            duration += (participants / 2) * exercise_time

        if participants > 8:
            duration += 8 * exercise_time

        duration += self.BREAK_BETWEEN_GROUPS

        return duration

    def create_stages_for_group(self, group_name: str, subgroup_name: str,
                                initial_participants: int,
                                otbor_exercise: str, polufinal_exercise: str, final_exercise: str) -> List[Stage]:
        stages = []
        group_id = f"{group_name}_{subgroup_name}"
        stage_order = 1

        if initial_participants > 19:
            if otbor_exercise:
                exercise_time = self.exercise_times.get(otbor_exercise, 0)
                duration = self.calculate_stage_duration(initial_participants, exercise_time)
                stages.append(Stage(
                    group_name=group_name,
                    subgroup_name=subgroup_name,
                    stage_type="отбор",
                    participants=initial_participants,
                    duration_minutes=duration,
                    exercises=[otbor_exercise],
                    stage_order=stage_order,
                    group_id=group_id
                ))
                stage_order += 1

            # После отбора остается 19 участников
            current_participants = 19
        else:
            current_participants = initial_participants

        if current_participants > 8:
            # Полуфинал - используем упражнение для полуфинала
            if polufinal_exercise:
                exercise_time = self.exercise_times.get(polufinal_exercise, 0)
                duration = self.calculate_stage_duration(current_participants, exercise_time)
                stages.append(Stage(
                    group_name=group_name,
                    subgroup_name=subgroup_name,
                    stage_type="полуфинал",
                    participants=current_participants,
                    duration_minutes=duration,
                    exercises=[polufinal_exercise],
                    stage_order=stage_order,
                    group_id=group_id
                ))
                stage_order += 1

            # После полуфинала остается 8 участников
            current_participants = 8

        # Финал (всегда есть) - используем упражнение для финала
        if final_exercise:
            exercise_time = self.exercise_times.get(final_exercise, 0)
            duration = self.calculate_stage_duration(current_participants, exercise_time)
            stages.append(Stage(
                group_name=group_name,
                subgroup_name=subgroup_name,
                stage_type="финал",
                participants=current_participants,
                duration_minutes=duration,
                exercises=[final_exercise],
                stage_order=stage_order,
                group_id=group_id
            ))

        return stages

    def load_all_stages(self) -> List[Stage]:
        df = pd.read_excel(self.processed_data_file, header=None)

        all_stages = []

        # Проходим по всем строкам (первая строка может быть заголовком, но мы её пропустим по условиям)
        for idx in range(len(df)):
            # Проверяем наличие данных в строке
            if df.shape[1] > 2 and pd.notna(df.iloc[idx, 0]) and pd.notna(df.iloc[idx, 1]):
                group_name = str(df.iloc[idx, 0]).strip()
                subgroup_name = str(df.iloc[idx, 1]).strip()

                # Пропускаем заголовки
                if 'наименование группы' in group_name.lower() or 'подгруппа' in subgroup_name.lower():
                    continue

                # Получаем количество участников
                participants = df.iloc[idx, 2] if df.shape[1] > 2 else 0
                if pd.isna(participants):
                    continue

                try:
                    participants = int(float(participants))
                except (ValueError, TypeError):
                    continue

                if participants <= 0:
                    continue

                # Получаем упражнения для этапов
                otbor_exercise = ''
                polufinal_exercise = ''
                final_exercise = ''

                if df.shape[1] > 3 and pd.notna(df.iloc[idx, 3]):
                    otbor_exercise = str(df.iloc[idx, 3]).strip()

                if df.shape[1] > 4 and pd.notna(df.iloc[idx, 4]):
                    polufinal_exercise = str(df.iloc[idx, 4]).strip()

                if df.shape[1] > 5 and pd.notna(df.iloc[idx, 5]):
                    final_exercise = str(df.iloc[idx, 5]).strip()

                # Создаем этапы для группы
                stages = self.create_stages_for_group(
                    group_name, subgroup_name, participants,
                    otbor_exercise, polufinal_exercise, final_exercise
                )
                all_stages.extend(stages)

        return all_stages

    def distribute_to_courts(self, stages: List[Stage], start_time: datetime) -> List[ScheduleSlot]:
        groups_stages: Dict[str, List[Stage]] = {}
        for stage in stages:
            if stage.group_id not in groups_stages:
                groups_stages[stage.group_id] = []
            groups_stages[stage.group_id].append(stage)

        # Сортируем этапы в каждой группе по порядку
        for group_id in groups_stages:
            groups_stages[group_id].sort(key=lambda s: s.stage_order)

        # Инициализируем корты (время окончания последнего выступления)
        court_end_times = {1: start_time, 2: start_time, 3: start_time}
        court_schedules = {1: [], 2: [], 3: []}

        # Отслеживаем последний запланированный этап для каждой группы
        last_scheduled_stage: Dict[str, Tuple[int, datetime]] = {}  # group_id -> (court, end_time)

        # Сортируем группы по общей длительности (самые длинные первые)
        sorted_groups = sorted(
            groups_stages.items(),
            key=lambda x: sum(s.duration_minutes for s in x[1]),
            reverse=True
        )

        # Распределяем этапы
        for group_id, group_stages in sorted_groups:
            for stage in group_stages:
                # Определяем, на каком корте должен быть этот этап
                if stage.stage_order == 1:
                    # Первый этап - выбираем корт с наименьшим временем окончания
                    available_court = min(court_end_times.items(), key=lambda x: x[1])[0]
                    stage_start = court_end_times[available_court]
                else:
                    # Последующие этапы - на том же корте после предыдущего этапа
                    prev_court, prev_end = last_scheduled_stage[group_id]
                    available_court = prev_court
                    stage_start = prev_end

                # Проверяем, не попадает ли на обед
                stage_start = self._adjust_for_lunch(stage_start, stage.duration_minutes)

                stage_end = stage_start + timedelta(minutes=stage.duration_minutes)

                # Создаем слот
                slot = ScheduleSlot(
                    court=available_court,
                    start_time=stage_start,
                    end_time=stage_end,
                    stage=stage
                )

                court_schedules[available_court].append(slot)
                court_end_times[available_court] = stage_end
                last_scheduled_stage[group_id] = (available_court, stage_end)

        # Объединяем все слоты и сортируем по времени и корту
        all_slots = []
        for court, slots in court_schedules.items():
            all_slots.extend(slots)

        all_slots.sort(key=lambda x: (x.start_time, x.court))

        return all_slots

    def _adjust_for_lunch(self, start_time: datetime, duration_minutes: float) -> datetime:
        lunch_start_min = self.LUNCH_START - self.LUNCH_TOLERANCE  # 12:30
        lunch_end_min = self.LUNCH_START + self.LUNCH_TOLERANCE + self.LUNCH_DURATION  # 13:60 = 14:00

        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = start_minutes + duration_minutes

        # Если выступление попадает на обед, переносим после обеда
        if start_minutes < lunch_end_min and end_minutes > lunch_start_min:
            # Переносим на время после обеда
            new_start_minutes = lunch_end_min
            new_start_time = start_time.replace(
                hour=new_start_minutes // 60,
                minute=new_start_minutes % 60,
                second=0
            )
            return new_start_time

        return start_time

    def generate_schedule(self, start_time_str: str) -> List[ScheduleSlot]:
        # Парсим время начала
        hour, minute = map(int, start_time_str.split(':'))
        start_time = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)

        # Загружаем все этапы
        all_stages = self.load_all_stages()

        if not all_stages:
            return []

        # Распределяем по кортам
        schedule = self.distribute_to_courts(all_stages, start_time)

        return schedule

    def format_schedule_as_text(self, schedule: List[ScheduleSlot], court_num: int) -> str:
        court_slots = [slot for slot in schedule if slot.court == court_num]

        if not court_slots:
            return f"Корт {court_num}: Нет выступлений"

        # Сортируем по времени
        court_slots.sort(key=lambda x: x.start_time)

        text = f"*КОРТ {court_num}*\n"
        text += "━" * 50 + "\n\n"

        # Группируем слоты по времени начала и группе
        current_time = None
        current_group = None
        stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}
        prev_hour = None

        for i, slot in enumerate(court_slots):
            time_str = slot.start_time.strftime('%H:%M')
            current_hour = slot.start_time.hour

            # Проверяем, нужно ли вставить обед (переход через 13:00)
            if prev_hour is not None and prev_hour < 13 and current_hour >= 13:
                # Выводим накопленное перед обедом
                if current_time:
                    text += self._format_group_block(current_time, current_group, stages_by_type)
                    stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}
                text += "\n🍽 *ОБЕД (13:00 - 13:30)*\n\n"

            # Если новое время или новая группа - выводим накопленное
            if (time_str != current_time or slot.stage.group_name != current_group) and current_time is not None:
                text += self._format_group_block(current_time, current_group, stages_by_type)
                stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}

            # Накапливаем этапы
            stages_by_type[slot.stage.stage_type].append(slot.stage)
            current_time = time_str
            current_group = slot.stage.group_name
            prev_hour = current_hour

        # Выводим последний блок
        if current_time:
            text += self._format_group_block(current_time, current_group, stages_by_type)

        return text

    def _format_group_block(self, time: str, group: str, stages_by_type: dict) -> str:
        text = f"⏰ *{time}* — {group}\n"

        # Отбор
        if stages_by_type["отбор"]:
            subgroups = [s.subgroup_name for s in stages_by_type["отбор"]]
            text += f"   📍 Отбор: {', '.join(subgroups)}\n"

        # Полуфинал
        if stages_by_type["полуфинал"]:
            subgroups = [s.subgroup_name for s in stages_by_type["полуфинал"]]
            text += f"   🥈 Полуфинал: {', '.join(subgroups)}\n"

        # Финал
        if stages_by_type["финал"]:
            subgroups = [s.subgroup_name for s in stages_by_type["финал"]]
            # Получаем упражнения из первого этапа (они одинаковые для группы)
            exercises = stages_by_type["финал"][0].exercises if stages_by_type["финал"] else []
            exercises_str = ", ".join(exercises) if exercises else "—"
            text += f"   🥇 Финал: {', '.join(subgroups)}\n"
            text += f"      Пхумсе: _{exercises_str}_\n"

        text += "\n"
        return text

    def save_schedule_to_excel(self, schedule: List[ScheduleSlot], output_file: str = None):
        """Сохраняет расписание в Excel файл"""
        if output_file is None:
            output_file = self.excel_file.replace('.xlsx', '_generated.xlsx')

        # Группируем по кортам
        court_schedules = {1: [], 2: [], 3: []}
        for slot in schedule:
            court_schedules[slot.court].append(slot)

        # Создаем Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for court_num in [1, 2, 3]:
                slots = court_schedules[court_num]

                # Формируем данные для листа
                data = []
                for slot in slots:
                    data.append({
                        'Время': slot.start_time.strftime('%H:%M'),
                        'Группа': slot.stage.group_name,
                        'Подгруппа': slot.stage.subgroup_name,
                        'Этап': slot.stage.stage_type,
                        'Участников': slot.stage.participants,
                        'Длительность (мин)': round(slot.stage.duration_minutes, 1),
                        'Окончание': slot.end_time.strftime('%H:%M'),
                        'Пхумсе': ', '.join(slot.stage.exercises)
                    })

                df = pd.DataFrame(data)
                sheet_name = f'Корт {court_num}'
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        return output_file
