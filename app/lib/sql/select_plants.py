plant_query = """
                    SELECT DISTINCT
                            td.plant_id
                    FROM ES_TAGS_DIM td
                    JOIN ES_PLANT_DIM pd
                        ON td.PLANT_ID = pd.PLANT_ID
                        AND pd.status = 'A'
                    """

if len(input_plant_ids) > 0:

    if len(input_plant_ids) > 1:
        plant_query = plant_query + """
                                    WHERE td.PLANT_ID IN {PlantIDs}
                                    """.format(PlantIDs=input_plant_ids)
    else:
        plant_query = plant_query + """
                                    WHERE td.PLANT_ID = {PlantID}
                                    """.format(PlantID=input_plant_ids[0])


# Get plant info by sqlalchemy-core:
plant_resp = es_dw_conn.execute(plant_query)

return [int(float(pid_rec[0])) for pid_rec in plant_resp.fetchall()]

