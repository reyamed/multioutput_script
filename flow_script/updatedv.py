def update_index_pattern(obj: Dict[str, Any]) -> bool:
    """Met à jour un index-pattern avec le nouveau titre, dans son espace."""
    obj_id = obj["id"]
    space_id = obj.get("_space_id", "default")
    prefix = "" if space_id == "default" else f"/s/{space_id}"

    old_title = obj["attributes"]["title"]        # déjà présent dans le _find
    new_title = build_new_title(old_title)
    if old_title == new_title:
        log.info(f"Skipping {obj_id} (space {space_id}) - already CCS-prefixed")
        return True

    put_url = f"{KIBANA_URL}{prefix}/api/saved_objects/index-pattern/{obj_id}"
    put_resp = session.put(put_url, json={"attributes": {"title": new_title}})
    if put_resp.status_code in (200, 201):
        log.info(f"Updated {obj_id} (space {space_id}): '{old_title}' -> '{new_title}'")
        return True
    log.error(f"Failed to update {obj_id} (space {space_id}): "
              f"{put_resp.status_code} {put_resp.text}")
    return False

def fetch_all_index_patterns() -> List[Dict[str, Any]]:
    """Récupère tous les index-pattern sur l'ensemble des espaces, avec pagination.
    Chaque objet est annoté avec son espace d'origine (_space_id)."""
    all_objects: List[Dict[str, Any]] = []
    seen_ids = set()

    resp = session.get(f"{KIBANA_URL}/api/spaces/space")
    if resp.status_code != 200:
        log.error(f"Cannot list spaces: {resp.status_code} {resp.text}")
        sys.exit(1)
    spaces = resp.json()

    for space in spaces:
        space_id = space["id"]
        prefix = "" if space_id == "default" else f"/s/{space_id}"
        page = 1              # reset per space
        space_count = 0       # per-space counter, drives pagination
        while True:
            url = (f"{KIBANA_URL}{prefix}/api/saved_objects/_find"
                   f"?type=index-pattern&per_page={PAGE_SIZE}&page={page}")
            r = session.get(url)
            if r.status_code != 200:
                log.error(f"Failed to fetch (space {space_id}, page {page}): "
                          f"{r.status_code} {r.text}")
                sys.exit(1)

            data = r.json()
            objects = data.get("saved_objects", [])
            for obj in objects:
                obj["_space_id"] = space_id
                if obj["id"] not in seen_ids:     # dedupe shared objects
                    seen_ids.add(obj["id"])
                    all_objects.append(obj)

            space_count += len(objects)
            total = data.get("total", 0)
            log.info(f"Fetched {space_count}/{total} (space {space_id}, page {page})")
            if space_count >= total or not objects:
                break
            page += 1

    log.info(f"Total unique index-patterns retrieved: {len(all_objects)}")
    return all_objects