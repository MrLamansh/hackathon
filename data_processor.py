import pandas as pd
from typing import Dict, List, Tuple


class DataProcessor:
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.groups_df = None
        self.exercises_df = None
        self.intermediate_df = None

    def load_data(self) -> bool:
        try:
            #Загружает лист 1 -  Группы и участники
            self.groups_df = pd.read_excel(
                self.input_file,
                sheet_name=0,  #Первый лист
                header=None
            )

            #Загружает лист 2 - Упражнения
            self.exercises_df = pd.read_excel(
                self.input_file,
                sheet_name=1,  #Второй лист
                header=None
            )

            return True
        except Exception as e:
            print(f"Ошибка при загрузке файла: {e}")
            return False

    def get_unique_exercises(self) -> List[str]:
        if self.exercises_df is None:
            return []

        exercises = set()

        for col_idx in [1, 2, 3]:
            if self.exercises_df.shape[1] > col_idx:
                for val in self.exercises_df.iloc[1:, col_idx]:
                    if pd.notna(val) and isinstance(val, str):
                        val = val.strip()
                        if val and val.lower() != 'nan':
                            exercises.add(val)

        return sorted(list(exercises))

    def find_group_exercises(self, group_name: str) -> Tuple[str, str, str]:
        if self.exercises_df is None:
            return ('', '', '')

        for idx in range(1, len(self.exercises_df)):  # Пропускаем заголовок
            if pd.notna(self.exercises_df.iloc[idx, 0]):
                group_in_exercises = str(self.exercises_df.iloc[idx, 0]).strip()

                if group_name.strip() == group_in_exercises:
                    otbor = ''
                    polufinal = ''
                    final = ''

                    if self.exercises_df.shape[1] > 1 and pd.notna(self.exercises_df.iloc[idx, 1]):
                        otbor = str(self.exercises_df.iloc[idx, 1]).strip()

                    if self.exercises_df.shape[1] > 2 and pd.notna(self.exercises_df.iloc[idx, 2]):
                        polufinal = str(self.exercises_df.iloc[idx, 2]).strip()

                    if self.exercises_df.shape[1] > 3 and pd.notna(self.exercises_df.iloc[idx, 3]):
                        final = str(self.exercises_df.iloc[idx, 3]).strip()

                    return (otbor, polufinal, final)

        return ('', '', '')

    def create_intermediate_data(self) -> bool:
        if self.groups_df is None or self.exercises_df is None:
            return False

        intermediate_data = []

        for idx in range(1, len(self.groups_df)):
            if pd.notna(self.groups_df.iloc[idx, 0]):
                group_name = str(self.groups_df.iloc[idx, 0]).strip()

                subgroup = ''
                if self.groups_df.shape[1] > 1 and pd.notna(self.groups_df.iloc[idx, 1]):
                    subgroup = str(self.groups_df.iloc[idx, 1]).strip()

                participants = 0
                if self.groups_df.shape[1] > 2 and pd.notna(self.groups_df.iloc[idx, 2]):
                    try:
                        participants = int(float(self.groups_df.iloc[idx, 2]))
                    except (ValueError, TypeError):
                        participants = 0

                otbor, polufinal, final = self.find_group_exercises(group_name)

                # Добавляем в промежуточные данные
                intermediate_data.append({
                    'Наименование группы': group_name,
                    'подгруппа': subgroup,
                    'Количество участников': participants,
                    'отбор': otbor if otbor else None,
                    'полуф': polufinal if polufinal else None,
                    'финал': final if final else None
                })

        self.intermediate_df = pd.DataFrame(intermediate_data)

        return True

    def save_intermediate_data(self, output_file: str = 'processed_data.xlsx') -> str:
        if self.intermediate_df is None:
            raise ValueError("Промежуточные данные не созданы. Сначала вызовите create_intermediate_data()")

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            self.intermediate_df.to_excel(writer, index=False, header=False)

        return output_file

    def get_intermediate_dataframe(self) -> pd.DataFrame:
        return self.intermediate_df

    def process(self, output_file: str = 'processed_data.xlsx') -> Tuple[bool, str, List[str]]:
        #Загрузка данных
        if not self.load_data():
            return (False, '', [])

        exercises = self.get_unique_exercises()

        if not self.create_intermediate_data():
            return (False, '', exercises)

        try:
            saved_file = self.save_intermediate_data(output_file)
            return (True, saved_file, exercises)
        except Exception as e:
            print(f"Ошибка при сохранении: {e}")
            return (False, '', exercises)


if __name__ == "__main__":
    processor = DataProcessor("user_input.xlsx")
    success, output_file, exercises = processor.process()

    if success:
        print(f"Обработка завершена успешно!")
        print(f"Выходной файл: {output_file}")
        print(f"Найдены упражнения: {exercises}")
    else:
        print("Ошибка при обработке данных")
