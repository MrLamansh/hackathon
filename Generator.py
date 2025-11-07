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
    BREAK_BETWEEN_GROUPS = 2
    LUNCH_START = 13 * 60
    LUNCH_DURATION = 30
    LUNCH_TOLERANCE = 30

    def __init__(self, excel_file: str):
        self.excel_file = excel_file
        self.exercise_times: Dict[str, float] = {}

    def get_unique_exercises(self) -> List[str]:
        df_all = pd.read_excel(self.excel_file, sheet_name="all", header=None)

        exercises = set()
        for col in [2, 3, 4]:
            if df_all.shape[1] > col:
                for val in df_all.iloc[:, col]:
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

    def get_group_exercises(self, group_name: str) -> List[str]:
        df_all = pd.read_excel(self.excel_file, sheet_name="all", header=None)

        for i in range(len(df_all)):
            if df_all.shape[1] > 1 and pd.notna(df_all.iloc[i, 1]):
                if group_name in str(df_all.iloc[i, 1]):
                    exercises = []
                    for col in [2, 3, 4]:
                        if df_all.shape[1] > col and pd.notna(df_all.iloc[i, col]):
                            val = str(df_all.iloc[i, col]).strip()
                            if val and val.lower() != 'nan':
                                exercises.append(val)
                    return exercises
        return []

    def create_stages_for_group(self, group_name: str, subgroup_name: str, initial_participants: int, exercises: List[str]) -> List[Stage]:
        stages = []
        group_id = f"{group_name}_{subgroup_name}"

        avg_exercise_time = sum(self.exercise_times.get(ex, 0) for ex in exercises) / len(exercises) if exercises else 0

        stage_order = 1

        if initial_participants > 19:
            duration = self.calculate_stage_duration(initial_participants, avg_exercise_time)
            stages.append(Stage(
                group_name=group_name,
                subgroup_name=subgroup_name,
                stage_type="отбор",
                participants=initial_participants,
                duration_minutes=duration,
                exercises=exercises,
                stage_order=stage_order,
                group_id=group_id
            ))
            stage_order += 1

            current_participants = 19
        else:
            current_participants = initial_participants

        if current_participants > 8:
            duration = self.calculate_stage_duration(current_participants, avg_exercise_time)
            stages.append(Stage(
                group_name=group_name,
                subgroup_name=subgroup_name,
                stage_type="полуфинал",
                participants=current_participants,
                duration_minutes=duration,
                exercises=exercises,
                stage_order=stage_order,
                group_id=group_id
            ))
            stage_order += 1

            current_participants = 8

        duration = self.calculate_stage_duration(current_participants, avg_exercise_time)
        stages.append(Stage(
            group_name=group_name,
            subgroup_name=subgroup_name,
            stage_type="финал",
            participants=current_participants,
            duration_minutes=duration,
            exercises=exercises,
            stage_order=stage_order,
            group_id=group_id
        ))

        return stages

    def load_all_stages(self) -> List[Stage]:
        df_prep = pd.read_excel(self.excel_file, sheet_name="prep", header=None)

        all_stages = []

        for idx in range(len(df_prep)):
            if df_prep.shape[1] > 5 and pd.notna(df_prep.iloc[idx, 1]) and pd.notna(df_prep.iloc[idx, 2]):
                group_name = str(df_prep.iloc[idx, 1]).strip()
                subgroup_name = str(df_prep.iloc[idx, 2]).strip()

                if 'группа' not in group_name.lower() or 'подгруппа' in subgroup_name.lower():
                    continue

                participants = df_prep.iloc[idx, 5]
                if pd.isna(participants) or not str(participants).replace('.', '').isdigit():
                    continue
                participants = int(float(participants))

                exercises = self.get_group_exercises(group_name)

                if exercises and participants > 0:
                    stages = self.create_stages_for_group(group_name, subgroup_name, participants, exercises)
                    all_stages.extend(stages)

        return all_stages

    def distribute_to_courts(self, stages: List[Stage], start_time: datetime) -> List[ScheduleSlot]:
        groups_stages: Dict[str, List[Stage]] = {}
        for stage in stages:
            if stage.group_id not in groups_stages:
                groups_stages[stage.group_id] = []
            groups_stages[stage.group_id].append(stage)

        for group_id in groups_stages:
            groups_stages[group_id].sort(key=lambda s: s.stage_order)

        court_end_times = {1: start_time, 2: start_time, 3: start_time}
        court_schedules = {1: [], 2: [], 3: []}

        last_scheduled_stage: Dict[str, Tuple[int, datetime]] = {}

        sorted_groups = sorted(groups_stages.items(), key=lambda x: sum(s.duration_minutes for s in x[1]), reverse=True)

        for group_id, group_stages in sorted_groups:
            for stage in group_stages:
                if stage.stage_order == 1:
                    available_court = min(court_end_times.items(), key=lambda x: x[1])[0]
                    stage_start = court_end_times[available_court]
                else:
                    prev_court, prev_end = last_scheduled_stage[group_id]
                    available_court = prev_court
                    stage_start = prev_end

                stage_start = self._adjust_for_lunch(stage_start, stage.duration_minutes)

                stage_end = stage_start + timedelta(minutes=stage.duration_minutes)

                slot = ScheduleSlot(court=available_court, start_time=stage_start, end_time=stage_end, stage=stage)

                court_schedules[available_court].append(slot)
                court_end_times[available_court] = stage_end
                last_scheduled_stage[group_id] = (available_court, stage_end)

        all_slots = []
        for court, slots in court_schedules.items():
            all_slots.extend(slots)

        all_slots.sort(key=lambda x: (x.start_time, x.court))

        return all_slots

    def _adjust_for_lunch(self, start_time: datetime, duration_minutes: float) -> datetime:
        lunch_start_min = self.LUNCH_START - self.LUNCH_TOLERANCE
        lunch_end_min = self.LUNCH_START + self.LUNCH_TOLERANCE + self.LUNCH_DURATION

        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = start_minutes + duration_minutes

        if start_minutes < lunch_end_min and end_minutes > lunch_start_min:
            new_start_minutes = lunch_end_min
            new_start_time = start_time.replace(
                hour=new_start_minutes // 60,
                minute=new_start_minutes % 60,
                second=0
            )
            return new_start_time

        return start_time

    def generate_schedule(self, start_time_str: str) -> List[ScheduleSlot]:
        hour, minute = map(int, start_time_str.split(':'))
        start_time = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)

        all_stages = self.load_all_stages()

        if not all_stages:
            return []

        schedule = self.distribute_to_courts(all_stages, start_time)

        return schedule

    def format_schedule_as_text(self, schedule: List[ScheduleSlot], court_num: int) -> str:
        court_slots = [slot for slot in schedule if slot.court == court_num]

        if not court_slots:
            return f"Корт {court_num}: Нет выступлений"

        court_slots.sort(key=lambda x: x.start_time)

        text = f"*КОРТ {court_num}*\n"
        text += "━" * 50 + "\n\n"

        current_time = None
        current_group = None
        stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}
        prev_hour = None

        for i, slot in enumerate(court_slots):
            time_str = slot.start_time.strftime('%H:%M')
            current_hour = slot.start_time.hour

            if prev_hour is not None and prev_hour < 13 <= current_hour:
                if current_time:
                    text += self._format_group_block(current_time, current_group, stages_by_type)
                    stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}
                text += "\n🍽 *ОБЕД (13:00 - 13:30)*\n\n"

            if (time_str != current_time or slot.stage.group_name != current_group) and current_time is not None:
                text += self._format_group_block(current_time, current_group, stages_by_type)
                stages_by_type = {"отбор": [], "полуфинал": [], "финал": []}

            stages_by_type[slot.stage.stage_type].append(slot.stage)
            current_time = time_str
            current_group = slot.stage.group_name
            prev_hour = current_hour

        if current_time:
            text += self._format_group_block(current_time, current_group, stages_by_type)

        return text

    def _format_group_block(self, time: str, group: str, stages_by_type: dict) -> str:
        text = f"⏰ *{time}* — {group}\n"

        if stages_by_type["отбор"]:
            subgroups = [s.subgroup_name for s in stages_by_type["отбор"]]
            text += f"   📍 Отбор: {', '.join(subgroups)}\n"

        if stages_by_type["полуфинал"]:
            subgroups = [s.subgroup_name for s in stages_by_type["полуфинал"]]
            text += f"   🥈 Полуфинал: {', '.join(subgroups)}\n"

        if stages_by_type["финал"]:
            subgroups = [s.subgroup_name for s in stages_by_type["финал"]]

            exercises = stages_by_type["финал"][0].exercises if stages_by_type["финал"] else []
            exercises_str = ", ".join(exercises) if exercises else "—"
            text += f"   🥇 Финал: {', '.join(subgroups)}\n"
            text += f"      Пхумсе: _{exercises_str}_\n"

        text += "\n"
        return text

    def save_schedule_to_excel(self, schedule: List[ScheduleSlot], output_file: str = None):
        if output_file is None:
            output_file = self.excel_file.replace('.xlsx', '_generated.xlsx')

        court_schedules = {1: [], 2: [], 3: []}
        for slot in schedule:
            court_schedules[slot.court].append(slot)

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for court_num in [1, 2, 3]:
                slots = court_schedules[court_num]

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
