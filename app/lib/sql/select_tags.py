tag_query = """
	    SELECT DISTINCT
		td.tag_name
	    FROM ES_TAGS_DIM td
	    WHERE td.plant_id = {PlantID}
	""".format(PlantID=plant_id)

# Get plant info by sqlalchemy-core:
tag_resp = es_dw_conn.execute(tag_query)

return [str(tag_name) for tag_name in tag_resp.fetchall()]

