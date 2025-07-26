TRUNCATE TABLE [DA_MDDE].[ConfigModelInfo]
GO
INSERT INTO
    [DA_MDDE].[ConfigModelInfo] (
        [FilenamePowerDesigner],
        [FilenameRepo],
        [Creator],
        [DateCreated],
        [Modifier],
        [DateModified],
        [OrderProcessed],
        [FileRETW],
        [FileRETWCreationDate],
        [FileRETWModificationDate]
    ) {%- for info_model in info_models %}
    SELECT
        '{{info_model.Filename}}',
        '{{info_model.FilenameRepo}}',
        '{{info_model.Creator}}',
        '{{info_model.DateCreated}}',
        '{{info_model.Modifier}}',
        '{{info_model.DateModified}}',
        '{{info_model.Order}}',
        '{{info_model.FileRETW}}',
        '{{info_model.FileRETWCreationDate}}',
        '{{info_model.FileRETWModificationDate}}' {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}

    GO
