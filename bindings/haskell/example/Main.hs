module Main where

import Database.NessDB
import Data.Maybe

-- quick & dirty test
main :: IO ()
main = do
    mdb <- db_open "ness_db_data"
    case mdb of 
        Just db -> do 
            db_add db "key" "val"
            db_add db "key'" "val'"

            res <- db_get db "key"
            case res of
                Just a -> print $ "value is " ++ a ++ "(Expected)"
                Nothing -> print "not found"

            res <- db_get db "key_not_exist"
            case res of Just val -> print $ "sth out of air! " ++ show val
                        Nothing -> print "not found (Expected)"
            db_close db
        Nothing ->
            print "open db failed. Drop me a letter (if path is writable && disk is not full)"



