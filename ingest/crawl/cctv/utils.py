def getAPCid(camera_row: tuple):
    camera_code = str(camera_row[0]).strip() if len(camera_row) > 0 else ""
    route_slug = str(camera_row[1]).strip() if len(camera_row) > 1 else ""

    if route_slug:
        return route_slug.upper()

    if camera_code.upper().startswith("APC"):
        return camera_code.upper().rstrip("/")

    return f"APC{camera_code}".upper()