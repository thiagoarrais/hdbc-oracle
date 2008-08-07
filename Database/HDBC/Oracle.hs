module Database.HDBC.Oracle (connectOracle) where

import Data.Maybe(fromJust)
import System.Time(ClockTime(TOD), toClockTime)
import Foreign.C.String(CString, peekCString)
import Foreign.C.Types(CInt)
import Foreign.Ptr(castPtr)
import Foreign.ForeignPtr(withForeignPtr)
import Numeric(showHex)

import Database.HDBC (IConnection(..), SqlValue)
import Database.HDBC.Statement (Statement(..), SqlValue(..))
import Database.HDBC.Oracle.OCIFunctions (EnvHandle, ErrorHandle, ConnHandle,
                                          ServerHandle, SessHandle, StmtHandle,
                                          ColumnInfo, ParamHandle, catchOCI,
                                          envCreate, terminate, defineByPos,
                                          serverAttach, serverDetach,
                                          handleAlloc, handleFree, getParam,
                                          setHandleAttr, getHandleAttr,
                                          setHandleAttrString, stmtPrepare,
                                          stmtExecute, stmtFetch,
                                          sessionBegin, sessionEnd,
                                          descriptorFree, formatErrorMsg,
                                          bufferToString, bufferToCaltime,
                                          bufferToDouble)
import Database.HDBC.Oracle.OCIConstants (oci_HTYPE_ERROR, oci_HTYPE_SERVER,
                                          oci_HTYPE_SVCCTX, oci_HTYPE_SESSION,
                                          oci_HTYPE_TRANS, oci_HTYPE_ENV,
                                          oci_HTYPE_STMT, oci_ATTR_SERVER,
                                          oci_ATTR_USERNAME, oci_ATTR_PASSWORD,
                                          oci_ATTR_SESSION, oci_ATTR_TRANS,
                                          oci_ATTR_PARAM_COUNT, oci_ATTR_NAME,
                                          oci_ATTR_DATA_TYPE,
                                          oci_DTYPE_PARAM, oci_NO_DATA,
                                          oci_CRED_RDBMS,
                                          oci_SQLT_CHR, oci_SQLT_AFC,
                                          oci_SQLT_AVC, oci_SQLT_DAT,
                                          oci_SQLT_NUM, oci_SQLT_FLT)

data OracleConnection = OracleConnection EnvHandle ErrorHandle ConnHandle
type ConversionInfo = (CInt, Int, ColumnInfo -> IO SqlValue)

instance IConnection OracleConnection where
    disconnect = disconnectOracle
    commit _ = fail "Not implemented"
    rollback _ = fail "Not implemented"
    run _ _ _ = fail "Not implemented"
    prepare = prepareOracle
    clone _ = fail "Not implemented"
    hdbcDriverName _ = "oracle"
    hdbcClientVer _ = fail "Not implemented"
    proxiedClientName _ = fail "Not implemented"
    proxiedClientVer _ = fail "Not implemented"
    dbServerVer _ = fail "Not implemented"
    dbTransactionSupport _ = False
    getTables _ = fail "Not implemented"
    describeTable _ _ = fail "Not implemented"

connectOracle :: String -> String -> String -> IO OracleConnection
connectOracle user pswd dbname = do
    env <- envCreate
    err <- createHandle oci_HTYPE_ERROR env
    server <- createHandle oci_HTYPE_SERVER env
    serverAttach err server dbname
    conn <- logToServer server user pswd env err
    return (OracleConnection env err conn)

logToServer :: ServerHandle -> String -> String -> EnvHandle -> ErrorHandle -> IO ConnHandle
logToServer srv user pswd env err = do
    conn <- createHandle oci_HTYPE_SVCCTX env
    session <- createHandle  oci_HTYPE_SESSION env
    trans <- createHandle oci_HTYPE_TRANS env
    setHandleAttr err (castPtr conn) oci_HTYPE_SVCCTX (castPtr srv) oci_ATTR_SERVER
    setHandleAttrString err (castPtr session) oci_HTYPE_SESSION user oci_ATTR_USERNAME
    setHandleAttrString err (castPtr session) oci_HTYPE_SESSION pswd oci_ATTR_PASSWORD
    sessionBegin err conn session oci_CRED_RDBMS
    setHandleAttr err (castPtr conn) oci_HTYPE_SVCCTX (castPtr session) oci_ATTR_SESSION
    setHandleAttr err (castPtr conn) oci_HTYPE_SVCCTX (castPtr trans) oci_ATTR_TRANS
    return conn

disconnectOracle (OracleConnection env err conn) = do
    session <- getHandleAttr err (castPtr conn) oci_HTYPE_SVCCTX oci_ATTR_SESSION
    server <- getHandleAttr err (castPtr conn) oci_HTYPE_SVCCTX oci_ATTR_SERVER
    sessionEnd err conn session
    serverDetach err server
    free session
    free server
    free conn
    free err
    free env

prepareOracle oraconn@(OracleConnection env err conn) query = do
    stmthandle <- createHandle oci_HTYPE_STMT env
    stmtPrepare err stmthandle query
    return (statementFor oraconn stmthandle query)

executeOracle :: OracleConnection -> StmtHandle -> [SqlValue] -> IO Integer
executeOracle (OracleConnection _ err conn) stmthandle bindvars = do
    stmtExecute err conn stmthandle 0
    return 0

getNumColumns err stmt = getHandleAttr err (castPtr stmt) oci_HTYPE_STMT oci_ATTR_PARAM_COUNT

dtypeConversion :: [(CInt, ConversionInfo)]
dtypeConversion = [(oci_SQLT_CHR, (oci_SQLT_CHR, 16000, readString)),
                   (oci_SQLT_AFC, (oci_SQLT_CHR, 16000, readString)),
                   (oci_SQLT_AVC, (oci_SQLT_CHR, 16000, readString)),
                   (oci_SQLT_DAT, (oci_SQLT_DAT, 7, readTime)),
                   (oci_SQLT_NUM, (oci_SQLT_FLT, 8, readNumber))]

fetchOracleRow :: OracleConnection -> StmtHandle -> IO (Maybe [SqlValue])
fetchOracleRow (OracleConnection _ err _) stmt = do
    numColumns <- getNumColumns err stmt
    readCols <- flip mapM [1..numColumns] $ \col -> do
        colHandle <- getParam err stmt col
        itype <- getHandleAttr err (castPtr colHandle) oci_DTYPE_PARAM oci_ATTR_DATA_TYPE
        let Just (otype, size, reader) = lookup itype dtypeConversion
        colinfo <- defineByPos err stmt col size otype
        return (reader colinfo)
    fr <- stmtFetch err stmt
    if fr == oci_NO_DATA
     then finishOracle stmt >> return Nothing
     else return . Just =<< sequence readCols

readString = readValue (\(_, buf, nullptr, sizeptr) -> bufferToString (undefined, buf, nullptr, sizeptr))
                       SqlString

readTime = readValue (\(_, buf, nullptr, sizeptr) -> bufferToCaltime nullptr buf)
                     (\time -> let TOD secs _ = toClockTime time in SqlEpochTime secs)

readNumber = readValue (\(_, buf, nullptr, sizeptr) -> bufferToDouble nullptr buf)
                       SqlDouble

readValue ::    (ColumnInfo -> IO (Maybe a))
             -> (a -> SqlValue)
             -> ColumnInfo -> IO SqlValue
readValue read convert colinfo = read colinfo >>= return . convert . fromJust

finishOracle :: StmtHandle -> IO ()
finishOracle = free

getOracleColumnNames :: OracleConnection -> StmtHandle -> IO [String]
getOracleColumnNames (OracleConnection _ err _) stmt = do
    numColumns <- getNumColumns err stmt
    flip mapM [1..numColumns] $ \col -> do
        colHandle <- getParam err stmt col
        str <- peekCString =<< getHandleAttr err (castPtr colHandle) oci_DTYPE_PARAM oci_ATTR_NAME
        free colHandle
        return str

createHandle htype env = handleAlloc htype (castPtr env) >>= return.castPtr
disposeHandle htype = handleFree htype . castPtr
disposeDescriptor = descriptorFree oci_DTYPE_PARAM . castPtr

class FreeableHandle h where free :: h -> IO ()

instance FreeableHandle EnvHandle where free = disposeHandle oci_HTYPE_ENV
instance FreeableHandle ErrorHandle where free = disposeHandle oci_HTYPE_ERROR
instance FreeableHandle ServerHandle where free = disposeHandle oci_HTYPE_SERVER
instance FreeableHandle ConnHandle where free = disposeHandle oci_HTYPE_SVCCTX
instance FreeableHandle SessHandle where free = disposeHandle oci_HTYPE_SESSION
instance FreeableHandle StmtHandle where free = disposeHandle oci_HTYPE_STMT
instance FreeableHandle ParamHandle where free = disposeDescriptor

statementFor oraconn stmthandle query =
    Statement {execute = executeOracle oraconn stmthandle,
               executeMany = \_ -> fail "Not implemented",
               finish = finishOracle stmthandle,
               fetchRow = fetchOracleRow oraconn stmthandle,
               originalQuery = query,
               getColumnNames = getOracleColumnNames oraconn stmthandle,
               describeResult = fail "Not implemented"}