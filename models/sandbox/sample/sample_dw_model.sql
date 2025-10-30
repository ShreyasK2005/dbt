    with src as
    (
        SELECT *
        from {{ ref('sample_ods_model') }} -- read from ods table
    )

    select *, -- transformations go here
        sysdate() as dw_create_datetime,
        sysdate() as dw_update_datetime
    from src
