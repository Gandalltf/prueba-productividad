# nomina_logic.py
# Lógica de nóminas: agrupa fichajes/productividad por día, hace top-ups hasta 7h
# y completa 6 días/semana usando horas de productividad cuando sea posible.

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict
import math

DOW = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
MONTH = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}


def _h(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(',', '.'))
    except Exception:
        return 0.0


def _dt(s):
    if isinstance(s, datetime):
        return s
    s = str(s).strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    # Intentos comunes
    for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # Último recurso: fromisoformat (puede lanzar)
    return datetime.fromisoformat(s)


def local_date(x, tz='Europe/Madrid'):
    z = ZoneInfo(tz)
    dt = _dt(x)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo('UTC'))
    return dt.astimezone(z).date()


def dr(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def wb(d: date):
    s = d - timedelta(days=d.weekday())
    e = s + timedelta(days=6)
    return s, e


def in_period(d, periods):
    """
    Devuelve True si la fecha d está dentro de alguno de los periodos flexibles
    (ignorando el año). Si periods está vacío / None, devuelve siempre False.
    """
    if not periods:
        return False

    for p in periods:
        try:
            s = _dt(p['start']).date()
            e = _dt(p['end']).date()
        except Exception:
            continue

        # Usamos (mes, día) para ignorar el año
        d_md = (d.month, d.day)
        s_md = (s.month, s.day)
        e_md = (e.month, e.day)

        # Rango normal (ej. 15 feb → 15 jun)
        if s_md <= e_md:
            if s_md <= d_md <= e_md:
                return True
        else:
            # Rango que cruza de año (ej. 15 nov → 15 feb)
            if d_md >= s_md or d_md <= e_md:
                return True

    return False


def r2(x):
    return math.floor(x * 100 + 0.5) / 100.0


def process(records, start_date, end_date, tz='Europe/Madrid', selected_worker=None,
            flexible_rest_periods=None, enforce_sunday_rest=True):

    # Normalizar flexible_rest_periods a una lista o lista vacía
    if not flexible_rest_periods:
        flexible_rest_periods = []
    elif isinstance(flexible_rest_periods, dict):
        flexible_rest_periods = [flexible_rest_periods]
    elif not isinstance(flexible_rest_periods, (list, tuple)):
        flexible_rest_periods = []

    start = _dt(start_date).date()
    end = _dt(end_date).date()

    # Normalizar y filtrar registros
    rows = []
    for r in records:
        try:
            f = local_date(r.get('fecha'), tz)
        except Exception:
            continue
        if not (start <= f <= end):
            continue

        name = r.get('trabajador') or r.get('TRABAJADOR') or r.get('Trabajador') or ''
        wid = r.get('trabajador_id')

        # Filtro por trabajador si se ha indicado
        if selected_worker not in (None, ''):
            if isinstance(selected_worker, (int, float)):
                if wid != selected_worker:
                    continue
            else:
                if str(name).strip().lower() != str(selected_worker).strip().lower():
                    continue

        cat = str(r.get('categoria') or r.get('CATEGORÍA') or r.get('CATEGORIA') or '').upper()
        cat = 'PRODUCTIVIDAD' if 'PROD' in cat else 'FICHAJE'

        rows.append({
            'trabajador': name,
            'trabajador_id': wid,
            'categoria': cat,
            'horas': _h(r.get('horas') or r.get('HORAS')),
            'fecha': f
        })

    by = defaultdict(list)
    for x in rows:
        key = x['trabajador'] or f"ID:{x['trabajador_id']}"
        by[key].append(x)

    out = {'workers': []}

    for worker, items in by.items():
        # Inicializa todos los días del rango
        days = {
            d: {
                'fichaje': 0.0,
                'prod': 0.0,
                'aj': 0.0,
                'nota': [],
                'dia': DOW[d.weekday()],
                'fecha': d.isoformat()
            }
            for d in dr(start, end)
        }

        # Cargar fichaje y productividad
        for it in items:
            d = it['fecha']
            if it['categoria'] == 'FICHAJE':
                days[d]['fichaje'] += it['horas']
            else:
                days[d]['prod'] += it['horas']

        # Pool de productividad (lo que podemos ir gastando)
        pool = {d: r2(v['prod']) for d, v in days.items()}

        def take(amt, prefer=None):
            rem = amt
            got = 0.0
            logs = []
            order = [prefer] if prefer else []
            order += [d for d in sorted(pool.keys()) if d != prefer]
            for d in order:
                if rem <= 0:
                    break
                av = pool.get(d, 0.0)
                if av <= 0:
                    continue
                t = min(rem, av)
                pool[d] = r2(av - t)
                rem = r2(rem - t)
                got = r2(got + t)
                logs.append({'from': d.isoformat(), 'hours': t})
            return got, logs, rem

        transfers = []

        # 1) Subir días < 7h a 7 (no tocar domingo fuera de periodo flexible)
        for d in sorted(days.keys()):
            flex = in_period(d, flexible_rest_periods)
            if enforce_sunday_rest and d.weekday() == 6 and not flex:
                continue
            fc = r2(days[d]['fichaje'])
            if fc >= 7.0:
                continue
            need = r2(7.0 - fc)
            got, logs, rem = take(need, prefer=d)
            if got > 0:
                days[d]['fichaje'] = r2(days[d]['fichaje'] + got)
                days[d]['aj'] = r2(days[d]['aj'] + got)
                transfers.append({
                    'to': d.isoformat(),
                    'hours': got,
                    'from_parts': logs,
                    'reason': 'Topup <7h'
                })

        # 2) Completar 6 días/semana con 7h (si hay productividad suficiente)
        seen = set()
        weeks = []
        for d in sorted(days.keys()):
            w = wb(d)  # (lunes, domingo)
            if w not in seen:
                seen.add(w)
                weeks.append(w)

        for s, e in weeks:
            # Días de esa semana que están dentro del rango [start, end]
            semana_dias = [d for d in dr(s, e) if d in days]
            if not semana_dias:
                continue

            flex = any(in_period(d, flexible_rest_periods) for d in semana_dias)

            # Días ya trabajados (ignorando domingo fuera de periodo flexible)
            worked = [
                d for d in semana_dias
                if r2(days[d]['fichaje']) > 0
                and not (enforce_sunday_rest and d.weekday() == 6 and not flex)
            ]
            if len(worked) >= 6:
                continue

            need = 6 - len(worked)

            # Candidatos para generar un día nuevo
            cand = [
                d for d in semana_dias
                if r2(days[d]['fichaje']) == 0
                and not (enforce_sunday_rest and d.weekday() == 6 and not flex)
            ]

            for d in cand:
                if need <= 0:
                    break
                if r2(sum(pool.values())) < 7.0:
                    break
                got, logs, rem = take(7.0, None)
                if got >= 7.0 - 1e-6:
                    days[d]['fichaje'] = r2(days[d]['fichaje'] + got)
                    days[d]['aj'] = r2(days[d]['aj'] + got)
                    days[d]['nota'].append('Día generado para completar 6 días de trabajo')
                    transfers.append({
                        'to': d.isoformat(),
                        'hours': got,
                        'from_parts': logs,
                        'reason': 'Completar 6 días'
                    })
                    need -= 1

        # Preparar salida
        tf = 0.0
        tp = 0.0
        avisos = []
        wdays = []
        for d in sorted(days.keys()):
            flex = in_period(d, flexible_rest_periods)
            if enforce_sunday_rest and d.weekday() == 6 and not flex and r2(days[d]['fichaje']) > 0:
                avisos.append(f'Domingo {d.isoformat()} con fichaje fuera de periodo flexible')
            prod_final = r2(pool.get(d, 0.0))
            fich_final = r2(days[d]['fichaje'])
            wdays.append({
                'fecha': d.isoformat(),
                'dia': days[d]['dia'],
                'fichaje': fich_final,
                'productividad': prod_final,
                'ajuste_desde_productividad': r2(days[d]['aj']),
                'notas': '; '.join(days[d]['nota']) if days[d]['nota'] else ''
            })
            tf += fich_final
            tp += prod_final

        filas = []
        for d in wdays:
            if d['fichaje'] > 0 or d['productividad'] > 0 or d['notas']:
                filas.append(
                    f"<tr><td>{d['dia']}</td><td>{d['fecha']}</td><td>{d['fichaje']}</td>"
                    f"<td>{d['productividad']}</td><td>{d['ajuste_desde_productividad']}</td>"
                    f"<td>{d['notas']}</td></tr>"
                )

        html = f"""
        <div style="font-family:Segoe UI,Arial,sans-serif;font-size:13px">
          <h3 style="margin:0 0 6px 0">{worker}</h3>
          <div style="color:#444;margin:0 0 8px 0">Periodo: {MONTH[start.month]} {start.year}</div>
          <table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;">
            <thead>
              <tr style="border-bottom:1px solid #ddd;text-align:left">
                <th>Día</th><th>Fecha</th><th>Fichaje (h)</th><th>Productividad (h)</th>
                <th>Transferido (h)</th><th>Notas</th>
              </tr>
            </thead>
            <tbody>
              {''.join(filas)}
            </tbody>
            <tfoot>
              <tr style="border-top:1px solid #ddd">
                <td colspan="2"><b>Totales</b></td>
                <td><b>{r2(tf)}</b></td>
                <td><b>{r2(tp)}</b></td>
                <td></td><td></td>
              </tr>
            </tfoot>
          </table>
        </div>
        """

        out['workers'].append({
            'trabajador': worker,
            'mes': MONTH[start.month],
            'anio': start.year,
            'days': wdays,
            'totales': {'fichaje': r2(tf), 'productividad': r2(tp)},
            'transferencias': transfers,
            'avisos': avisos,
            'html': html
        })

    return out
