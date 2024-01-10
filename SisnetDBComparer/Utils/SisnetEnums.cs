namespace SisnetDBComparer.Utils
{
    public enum ColIndexComparer
    {
        ImageStatus,
        ImageComparer,
        Index,
        Tabla1,
        Counter1,
        Size1,
        Tabla2,
        Counter2,
        Size2,
    }

    public enum ColIndexDetail
    {
        ImageStatus,
        Index,
        Tabla1,
        Counter1,
        Size1,
        Tabla2,
        Counter2,
        Size2

    }

    public enum Status
    {
        Cargando,
        Negro,
        Azul,
        Verde,
        Amarillo,
        Equal,
        NotEqual
    }

    public enum Conexion
    {
        Conexion1,
        Conexion2
    }


    public enum StatusDetails
    {
        CargandoData1,
        CargandoData2,
        RefreshGrid,
        CargaCompleta,
        CargadoData1,
        ComparacionInicio,
        ComparacionOk,
        ComparacionBad,
        ComparacionFinished,
        SyncTable
    }

    public enum StatusSync
    {
        SyncDataStart,
        SyncDataFinished,
        SyncFinishTable,
        NoDataToSync,
        LoadingTables,
        SyncMovingData,
        SyncRow,
        SyncCreatingTable,
        SyncPreparingSentences,
        SyncReadingPackage,
        SyncPreparingPool,
        SyncCreateAllFK,
        SyncCreatedTable,
    }
}
