"""Utilidades para trabajar con polylines codificadas."""

import polyline


def decode_polyline_to_wkt(encoded_polyline: str) -> str:
    """Decodifica una polyline y la convierte en WKT LINESTRING.

    Args:
        encoded_polyline: Polyline codificada (formato Google).

    Returns:
        Cadena WKT con la geometría de la ruta.

    Raises:
        ValueError: Si la polyline no puede decodificarse.
    """
    try:
        coords = polyline.decode(encoded_polyline)
    except Exception as exc:
        raise ValueError("La polyline proporcionada no es válida.") from exc

    if len(coords) < 2:
        raise ValueError("La polyline debe contener al menos 2 coordenadas.")

    # polyline.decode devuelve (lat, lon); WKT necesita (lon lat).
    linestring_coords = ", ".join([f"{lon} {lat}" for lat, lon in coords])
    return f"LINESTRING({linestring_coords})"
