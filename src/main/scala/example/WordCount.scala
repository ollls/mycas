package example

import cas.mutable.SkipList
import cas.mutable.{ StringIgnoreCaseValuePair, MultiValuePairReversed }

import java.io.RandomAccessFile
import java.util.concurrent.{CountDownLatch, Future, ExecutorService, Callable, Executors  }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.Buffer


object RunMe {

  //exec wraper on lazy/unevalueated block.
  //example of block where T is Int {  val i1 = 10; val i2 = 12; ii + i2  }
  //explanation: if we write  (r : T ), T will be evaluated immediately and value 22 will be passed.
  //if we write ( r :=> T ), in our case is r :=> Int, block will be wrapped up into function and referred by pointer.
  //It will be evaluated only when needed, which is inside Callable::call() method.
  //This called a lazy evaluation. It is nothing but automatic wrapper function which returns value when needed, unless lazy keyword used again.  
  def exec[T]( es: ExecutorService)(r: => T) : Future[T] = es.submit(new Callable[T] { def call = r })



  //////////////////////////////////////////////////////
  def my_file_sz( fname : String ) : Long = {
    val f = new RandomAccessFile( fname, "rw")
    val l = f.length()
    f.close()
    l
  }



 /////////////////////////////////////////////////////////
 def file_task( acum :  SkipList[ StringIgnoreCaseValuePair[AtomicInteger]], fname : String,  offset : Long,  delta : Long,  cl : CountDownLatch ): Unit =
 {
    val f = new RandomAccessFile(fname, "r")
    try {
       var cur: Long = 0
       var bb1: Byte = ' '

       //skip a split word but if prev char was space, we got to the right position where a word begins
       if (offset != 0) {

          f.seek(offset - 1) //one char back, was it space before ?
          var SpaceOrNot = f.readByte()

          if (Character.isLetterOrDigit(SpaceOrNot) == true) {
             bb1 = f.readByte()
             while (Character.isLetterOrDigit(bb1)) {
                bb1 = f.readByte()
             }
          }
       }


     cur = f.getFilePointer()


     var word_count = 0;

    while (cur < offset + delta) {
      var buf11 = Buffer[Byte]()
      bb1 = f.readByte()
      var bb_prev: Byte = ' '

      while (Character.isLetterOrDigit(bb1) || bb1 == '\'') {
         bb_prev = bb1
         buf11.append(bb_prev)

         try {
          bb1 = f.readByte()
        } catch {
          case eof : java.io.EOFException => { bb1 = ' ' }
        }
      }


      if (Character.isLetterOrDigit(bb_prev)) {

      word_count = word_count + 1

      val str = new String(buf11.toArray, "CP1251")

      if ( str.charAt(0).isDigit == false ) {  //ignore anything wich starts with digit

         val opt = acum.get( StringIgnoreCaseValuePair( str, new AtomicInteger(0) ) )
         opt match {
            case None => {
                 val res = acum.add(StringIgnoreCaseValuePair(str, new AtomicInteger(1)))
                 //res - false due to Compare and Set constraint, someone added it before us, the word is already there, it happens somewhere else in the text
                 if( res == false )
                 {
                    //retrive direct atomic reference on occurences, new AtomicInteger(0) just a place holder, since get is executed on entire object
                    val opt = acum.get( StringIgnoreCaseValuePair( str, new AtomicInteger(0) ) )
                    opt.get.value.getAndIncrement()
                 }    
          }
          case Some(a) => a.value.getAndIncrement()
         }
      }
    }


  cur = f.getFilePointer()

}


} catch {  case eof : java.io.EOFException => { println( eof.toString)} }
  finally {  f.close(); cl.countDown() }

}


   ////////////////////////////////////////////////// 
   def word_count() : Unit = {

      val MY_FILE_PATH = "c:/1/pg10.txt";
      val THREADS   = 4
      val endSignal = new CountDownLatch( THREADS );
      val es = Executors.newFixedThreadPool( THREADS ); 

      //SkipList will contain objects of class StringIgnoreCaseValuePair or simply object which has a key and asociated value.
      //In that configuration, SkipList is eventualy is a backend for simple String -> Int Map.
      //StringIgnoreCaseValuePair implements implict comparator: Ordered, which provides proper ordering in SkipList.
      //In StringIgnoreCaseValuePair for a key of type String, no letter case is considered.
      //Each key is a word in the text file and it has a number of same word occurrences from that file as associated value.
      //The associated value needs to be Atomic, many threads will be by trying to increment it. 
      val SL = new SkipList[StringIgnoreCaseValuePair[AtomicInteger]]()

      
      val size = my_file_sz( "c:/1/pg10.txt" )
      println( "file length = " + size )

      if ( size < THREADS ) {
         println( "File is too small, exiting" )
         System.exit(0)
      }

      val dt           : Long = size / THREADS
      var start_offset : Long = 0;

      
      do {
         val offset = start_offset //copy for lazy eval in the thread, offset inside of lazy block passed to exec()
         exec(es) ( file_task( SL, MY_FILE_PATH, offset, dt, endSignal ) )
         start_offset = start_offset + dt
      } while( start_offset <= size - dt )


      endSignal.await() 


      //SL.debug_print_layers()
     
      println()
      println( "total = " + SL.count() )
      println( "head test "  + SL.head )

      //create another Map to be able to see number of occurrences for the actual word.  
      //we do reveresed to keep the biggest number first
      val SL_REV = new SkipList[MultiValuePairReversed[Int, String]]()

      SL.foreachFrom( SL.head,
      c => {
        SL_REV.add( new MultiValuePairReversed( c.value.get(), c.key ) )
      } )

      //Print first 27
      var counter = 0;
      SL_REV.foreach( c => println(  "%-10s %s".format( c.value, c.key ) ),
                    c =>  {
                      if ( counter > 12 ) false
                      else { counter = counter + 1; true }
                    }) 

         
                    
      println( "Stream test")
      
      //TODO: find a way to have simple find by key : String. for all the Pair classes with key String ???
      //here we need to provide entire pair to lookup.
      //Maybe we need a wrapper for all simple key String maps.
      //SL.toStream( new StringIgnoreCaseValuePair( "N", new AtomicInteger(0) )).take( 7 ).foreach( c => println( c.key ) ) 

      es.shutdown()              

    }



   def main( args : Array[String] ) = {
 
       word_count

   }


}


