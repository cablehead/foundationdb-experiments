seconds_per = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def convert_to_seconds(s):
    return int(s[:-1]) * seconds_per[s[-1]]


def humanize_time(amount, units = 'seconds'):
    def process_time(amount, units):
        INTERVALS = [   1, 60,
                        60*60,
                        60*60*24,
                        60*60*24*7,
                        60*60*24*7*4,
                        60*60*24*7*4*12,
                        60*60*24*7*4*12*100,
                        60*60*24*7*4*12*100*10]
        NAMES = [('second', 'seconds'),
                 ('minute', 'minutes'),
                 ('hour', 'hours'),
                 ('day', 'days'),
                 ('week', 'weeks'),
                 ('month', 'months'),
                 ('year', 'years'),
                 ('century', 'centuries'),
                 ('millennium', 'millennia')]

        result = []

        unit = [a[1] for a in NAMES].index(units)
        amount = amount * INTERVALS[unit]

        for i in range(len(NAMES)-1, -1, -1):
            a = amount // INTERVALS[i]
            if a > 0:
                result.append( (a, NAMES[i][1 % a]) )
                amount -= a * INTERVALS[i]

        return result

    rd = process_time(int(amount), units)
    cont = 0
    for u in rd:
        if u[0] > 0:
            cont += 1

    buf = ''
    i = 0
    for u in rd:
        if u[0] > 0:
            buf += "%d %s" % (u[0], u[1])
            cont -= 1

        if i < (len(rd)-1):
            if cont > 1:
                buf += ", "
            else:
                buf += " and "

        i += 1

    return buf
