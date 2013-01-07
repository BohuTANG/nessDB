{-# LANGUAGE CPP, ForeignFunctionInterface, GeneralizedNewtypeDeriving #-}

module Database.NessDB.Impl where

import Foreign
import Foreign.C.Types
import Foreign.C.String
import Control.Monad

#include "db.h"

newtype Status = Status { status :: CInt } deriving (Eq, Num, Show)

sOK :: Status
sOK = Status 1

sERR :: Status
sERR = Status 0

type Nessdb = Ptr ()

type RStatus = IO (Status)

data Slice = Slice { buffer :: Ptr CChar, buffer_len :: Int } deriving Show

instance Storable Slice where
    sizeOf    _ = #{size struct slice}

    alignment _ = alignment (undefined :: CInt)

    poke p foo  = do
        #{poke struct slice, data} p $ buffer foo
        #{poke struct slice, len} p $ buffer_len foo

    peek p = return Slice 
              `ap` (#{peek struct slice, data} p)
              `ap` (#{peek struct slice, len} p)
{--
--TODO
struct iter {
    int c;
    int idx;
    int valid;
    struct slice *key;
    struct slice *value;
    struct ness_kv *kvs;
};
--}


foreign import ccall unsafe "db_open"
    c_db_open :: Ptr CChar -> Nessdb

foreign import ccall unsafe "db_add"
    c_db_add :: Nessdb -> Ptr Slice -> Ptr Slice -> RStatus

foreign import ccall unsafe "db_get"
    c_db_get :: Nessdb -> Ptr Slice -> Ptr Slice -> RStatus

foreign import ccall unsafe "db_free_data"
    c_free_data :: Ptr CChar -> IO () 

{--
foreign import ccall unsafe "db_exists"
    c_db_exists :: Nessdb -> Ptr Slice -> RStatus
--}

foreign import ccall unsafe "db_stats"
    c_db_stats :: Nessdb -> Ptr Slice -> RStatus

foreign import ccall unsafe "db_remove"
    c_db_remove :: Nessdb -> Ptr Slice -> IO()

foreign import ccall unsafe "db_close"
    c_db_close :: Nessdb -> IO()

db_open' :: String -> IO Nessdb
db_open' path = withCString path $ return . c_db_open

db_open :: String -> IO (Maybe Nessdb)
db_open path = db_open' path >>= \h ->
                return (if h == nullPtr
                          then Nothing
                          else Just h)

db_add :: Nessdb -> String -> String -> RStatus 
db_add db key val = withCString key ( \k -> 
    withCString val (\v -> 
        let sk = Slice k $ length key 
            sv = Slice v $ length val 
        in with sk ( \psk ->
             with sv (c_db_add db psk))))
                -- look ma, am I speaking lisp in haskell?

db_get :: Nessdb -> String -> IO (Maybe String)
db_get db key = withCString key ( \k ->
    let sk = Slice k $ length key
    in with sk ( \pk ->
        alloca  ( \pv ->
             c_db_get db pk pv >>= \s ->
                 if s == sOK then
                     peek pv >>=
                         \v -> do
                            str <- peekCStringLen (buffer v, buffer_len v) 
                            c_free_data (buffer v)
                            return $ Just str
                 else
                    return Nothing)))

db_remove :: Nessdb -> String -> IO ()
db_remove db key = withCString key ( \k ->
        let sk = Slice k $ length key
        in with sk ( \pk -> c_db_remove db pk))

db_close :: Nessdb -> IO()
db_close = c_db_close

