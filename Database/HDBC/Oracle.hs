module Database.HDBC.Oracle (connectOracle) where

import Foreign.Ptr(castPtr)

import Database.HDBC (IConnection(..), SqlValue)
import Database.HDBC.Statement (Statement(..))
import Database.HDBC.Oracle.OCIFunctions (EnvHandle, ErrorHandle, ConnHandle,
                                          ServerHandle, SessHandle, StmtHandle,
                                          envCreate, terminate, defineByPos,
                                          serverAttach, serverDetach,
                                          handleAlloc, handleFree,
                                          setHandleAttr, getHandleAttr,
                                          setHandleAttrString, stmtPrepare,
                                          stmtExecute,
                                          sessionBegin, sessionEnd)
import Database.HDBC.Oracle.OCIConstants (oci_HTYPE_ERROR, oci_HTYPE_SERVER,
                                          oci_HTYPE_SVCCTX, oci_HTYPE_SESSION,
                                          oci_HTYPE_TRANS, oci_HTYPE_ENV,
                                          oci_HTYPE_STMT, oci_ATTR_SERVER,
                                          oci_ATTR_USERNAME, oci_ATTR_PASSWORD,
                                          oci_ATTR_SESSION, oci_ATTR_TRANS,
                                          oci_ATTR_PARAM_COUNT,
                                          oci_CRED_RDBMS)

data OracleConnection = OracleConnection EnvHandle ErrorHandle ConnHandle
                      | NullConnection

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
    return NullConnection

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
    terminate

prepareOracle oraconn@(OracleConnection env err conn) query = do
    stmthandle <- createHandle oci_HTYPE_STMT env
    stmtPrepare err stmthandle query
    return (statementFor oraconn stmthandle)

executeOracle :: OracleConnection -> StmtHandle -> [SqlValue] -> IO Integer
executeOracle (OracleConnection _ err conn) stmthandle bindvars = do
    stmtExecute err conn stmthandle 0
    return 0

fetchOracleRow :: OracleConnection -> StmtHandle -> IO (Maybe [SqlValue])
fetchOracleRow (OracleConnection _ err _) stmthandle = do
    (n :: Int) <- getHandleAttr err (castPtr stmthandle) oci_HTYPE_STMT oci_ATTR_PARAM_COUNT
    cols <- mapM (\pos -> defineByPos err stmthandle pos 0 0) [1..n]
    return Nothing

createHandle htype env = handleAlloc htype (castPtr env) >>= return.castPtr
disposeHandle htype = handleFree htype . castPtr

class FreeableHandle h where free :: h -> IO ()

instance FreeableHandle EnvHandle where free = disposeHandle oci_HTYPE_ENV
instance FreeableHandle ErrorHandle where free = disposeHandle oci_HTYPE_ERROR
instance FreeableHandle ServerHandle where free = disposeHandle oci_HTYPE_SERVER
instance FreeableHandle ConnHandle where free = disposeHandle oci_HTYPE_SVCCTX
instance FreeableHandle SessHandle where free = disposeHandle oci_HTYPE_SESSION

statementFor oraconn stmthandle =
    Statement {execute = executeOracle oraconn stmthandle,
               executeMany = \_ -> fail "Not implemented",
               finish = fail "Not implemented",
               fetchRow = fetchOracleRow oraconn stmthandle,
               originalQuery = fail "Not implemented",
               getColumnNames = fail "Not implemented",
               describeResult = fail "Not implemented"}