## DE-KP-RDD
# Требования
- scala:2.12.17
- java:1.8
- apache spark:3.4.0
- maven:latest
- $SPARK_HOME = .../spark-dir
# Запуск
Для запуска достаточно запустить скрипт ./start.sh
# Описание
- Программа выполняется локально с использованием максимального кол-ва ядер процессора ( -master local[*] )
- Используется RDD
- Результат сохраняется в файл result
- Формат результата 1 задания: Count: $c; где с - требуемое значение
- Формат результата 2 задания: $docHash\t$date\t$count  где
  - docHash - хеш от названия документа
  - date - дата открытия документа в формате unix epoch
  - count - кол-во открытий
  - Ключом для группировки является тройка  (docHash, date)         
# Проблемы
Если рассматривать файл как набор строк, то строки являются зависимыми друг от друга с точки зрения анализа сессии. Поэтому удобно рассматривать целый файл как еденицу RDD. Что допустимо с учетом размера файлов и их кол-ва.
Если рассматривать строку RDD как еденицу данных, то возникает проблема с зависимостями между ними (например если строки из одного файла попадут разным excutor)
Возникает 2 проблемы:
- большой размер файла
- слишком много файлов

Для заданных данных эти проблемы не возникают.  
Проблема большого кол-ва файлов можно решить за счет анализа только их части и объединения результата. Проблема большого файла обусловленна форматом данных и может быть частично решена за счет стриминга.
# Streaming
- Можно читать файлы по строка. Проблема необходимость последовательно анализировать прочитанные данные. Теряется параллельность. Решается проблема с большими файлами.
- Можно читать файлы целиком тогда обработка одного интервала повторяет представленное решение, за исключением необходимости объединения данных за каждый интервал.
- Основное преимущество стриминга. Потенциально бесконенчный анализ. Решается проблема с объемом прочитанных данных. Усложнения за счет необходимости хранить доп. данные за интервалы.
- Для данной задачи в стриминге нет необходимости.
