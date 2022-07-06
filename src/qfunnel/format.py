import itertools

def format_box_table(head, rows):
    return format_table(
        head,
        rows,
        vert=' │ ',
        horiz='─',
        cross='─┼─'
    )

def format_table(head, rows, vert, horiz, cross):
    widths = [
        max(len(row[col]) for row in itertools.chain([head], rows))
        for col in range(len(head))
    ]
    yield format_row(head, widths, vert)
    yield cross.join(horiz * w for w in widths)
    for row in rows:
        yield format_row(row, widths, vert)

def format_row(row, widths, sep):
    return sep.join('{:{}}'.format(x, w) for x, w in zip(row, widths))

def format_date(d):
    weekday = d.strftime('%a')
    month = d.strftime('%b')
    hour = d.hour % 12
    if hour == 0:
        hour = 12
    ampm = 'AM' if d.hour < 12 else 'PM'
    return f'{weekday} {month} {d.day} @ {hour}:{d.minute:02}:{d.second:02} {ampm}'
