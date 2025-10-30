    with src as
    (
        SELECT *
        from {{ source('erp', 'orders') }} -- read from raw table (today's data, files, etc.) based on source .yml files
    )

    select *,  -- typically very few transformations
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src