import datetime
import re

HEADING_RE = re.compile(r'(?:submit/start at|\S+)\ +')

def parse_table(s):
    lines = s.split('\n')
    while lines and not lines[-1]:
        lines.pop()
    if not lines:
        return []
    head, _, *rows = lines
    raw_headings = HEADING_RE.findall(head)
    min_lengths = [len(s) for s in raw_headings]
    headings = [s.rstrip(' ') for s in raw_headings]
    return parse_rows(rows, min_lengths, headings)

def parse_rows(rows, min_lengths, headings):
    for row in rows:
        values = parse_row(row, min_lengths)
        yield { h : v.strip(' ') for h, v in zip(headings, values) }

def parse_row(row, min_lengths):
    start = 0
    for length in min_lengths:
        end = start + length
        while end <= len(row) and row[end-1] != ' ':
            end += 1
        yield row[start:end]
        start = end

DATE_RE = re.compile(r'^(\d\d)/(\d\d)/(\d\d\d\d) (\d\d):(\d\d):(\d\d)$')

def parse_date(s):
    month, day, year, hour, minute, second = map(int, DATE_RE.match(s).groups())
    return datetime.datetime(year, month, day, hour, minute, second)

def get_job_id(row):
    job_id = row['job-ID']
    task_id = row['ja-task-ID']
    if task_id:
        job_id = f'{job_id}.{task_id}'
    return job_id

def get_since(row):
    return parse_date(row['submit/start at'])

EXPLAIN_SECTION_SEPARATOR = '=============================================================='

def parse_explain_sections(s):
    lines = s.split('\n')
    section = []
    for line in lines:
        if line == EXPLAIN_SECTION_SEPARATOR:
            if section:
                yield section
                section = []
        else:
            section.append(line)
    if section:
        yield section
