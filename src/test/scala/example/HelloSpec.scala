package example

import org.scalatest._

import org.scalatest.flatspec.AnyFlatSpec
import java.util.concurrent.CountDownLatch
import java.util.concurrent._
import cas.mutable._

class ListSpec extends AnyFlatSpec { // with Matchers {

  val NTOTAL = 1020311
  var startSignal : CountDownLatch = null
  var endSignal : CountDownLatch = null


  val es = Executors.newFixedThreadPool(1000);
  val SI = new SkipList[Int]

  //linear list search about 30
  SI.FACTOR = 10



  def exec[T](es: ExecutorService)(r: => T): Future[T] = es.submit(new Callable[T] {
    def call = r
  })
   
   "Test" should "be OK" in {

    val rt = java.lang.Runtime.getRuntime();

    println( "Total memory, MB:" + rt.totalMemory() /1024 /1024 )

    //rt.gc()

    var usedMB : Long = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    println( "\n#1 memory usage: " + usedMB + " MB\n");

    /////////////////////////////////
    parallel_add()

    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    println( "\n#2 memory usage: " + usedMB + " MB\n" );

    /////////////////////////////////
    parallel_cleanup_partial( )


    /////////////////////////////////
    //race_and_stress()
    parallel_add()    


    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    println( "\n#3 memory usage: " + usedMB + " MB\n" );

     
    ///////////////////////////////////////
    get_and_find()

    rt.gc()

    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    println( "\n#4 memory usage after GC: " + usedMB + " MB\n" );

    //SI.debug_print()



   }


   ///////////////////////////////////////////////
   def add_task_u_reverse(list: SkipList[Int], start: Int, i_by : Int ) = {
    try {
      startSignal.await()
      for ( i <-  ( start to NTOTAL by i_by).reverse ) {
        list.add(i)
      }

      endSignal.countDown()
    }
    catch
      {
        case e : Exception => e.printStackTrace()
      }

  }

  ///////////////////////////////////////////////
  def rem_task_u_reverse(list: SkipList[Int], start: Int, i_by : Int ) = {
    try {
      startSignal.await()
      for ( i <-  ( start to NTOTAL by i_by).reverse ) {

        list.remove( i )
      }
      endSignal.countDown()

    } catch
      {
        case e : Exception => e.printStackTrace()
      }
  }


  ///////////////////////////////////////////////
  def rem_task_ux2(list: SkipList[Int], start: Int, i_by : Int ) = {
    try {
      var delta : Long = 0
      var num = 0;
      startSignal.await()
      for (i <- start to NTOTAL - 1000 by i_by ) {
        val d1 = java.lang.System.currentTimeMillis();
        val res = list.remove( i )
        val d2 = java.lang.System.currentTimeMillis();

        if ( delta < ( d2 - d1 ) ) { delta = d2 - d1; num = i }

        if ( res == false )
        {
          println(  "rem_task_u(): not removed: " + i )
        }

      }
      println( delta + " ms, on "  + num) 
      endSignal.countDown()

    } catch
      {
        case e : Exception => e.printStackTrace()
      }
  } 

  ///////////////////////////////////////////////
  def rem_task_u(list: SkipList[Int], start: Int, i_by : Int ) = {
    try {
      var delta : Long = 0
      var num = 0;
      startSignal.await()
      for (i <- start to NTOTAL by i_by ) {
        val d1 = java.lang.System.currentTimeMillis();
        val res = list.remove( i )
        val d2 = java.lang.System.currentTimeMillis();

        if ( delta < ( d2 - d1 ) ) { delta = d2 - d1; num = i }

        if ( res == false )
        {
          //println(  "rem_task_u(): not removed: " + i )
        }

      }
      println( delta + " ms, on "  + num) 
      endSignal.countDown()

    } catch
      {
        case e : Exception => e.printStackTrace()
      }
  }


   ///////////////////////////////////////////////
   def rem_task_u_debug(list: SkipList[Int], start: Int, i_by : Int ) = {
    try {
      var delta : Long = 0
      var num = 0;
      startSignal.await()
      for (i <- start to NTOTAL by i_by ) {
        val d1 = java.lang.System.currentTimeMillis();
        println( "remove- > " + i)
        list.remove( i )
        val d2 = java.lang.System.currentTimeMillis();

        if ( delta < ( d2 - d1 ) ) { delta = d2 - d1; num = i }

      }

      endSignal.countDown()

    } catch
      {
        case e : Exception => e.printStackTrace()
      }
  }


    ///////////////////////////////////////////////
    def add_task_u(list: SkipList[Int], start: Int, i_by : Int ) = {
      try {
        startSignal.await()
        for ( i <- start to NTOTAL by i_by ) {
          list.add(i)
        }
  
        endSignal.countDown()
      }
      catch
        {
          case e : Exception => e.printStackTrace()
        }
  
    }


  def parallel_add( ) : Unit = {

    println( "Max Range = " + NTOTAL );

    println( "Parallel add test: 3 threads adding adjacent elements in the same time, resolving conflicts" )
    println( "Thread #1 works with sequence: 1, 4, 7..., Thread #2: 2, 5, 8..., Thread #3: 3, 6, 9...")
    println( "As a stress test we create twice as many threads doing teh same work, to introduce more congestion")
    startSignal = new CountDownLatch(1)
    endSignal = new CountDownLatch( 5 )


    exec( es)(  add_task_u( SI, 1, 3)  )
    exec( es)(  add_task_u( SI, 2, 3 ) )
    //exec( es)(  add_task_u( SI, 3, 3 ) )

    exec( es)(  add_task_u( SI, 1, 3)  )
    exec( es)(  add_task_u( SI, 2, 3 ) )
    exec( es)(  add_task_u( SI, 3, 3 ) )

    startSignal.countDown()

    endSignal.await()

    println ( "count=" + SI.count() )

    //SI.debug_print()

    println( "*** Layers ***")
    SI.debug_print_layers1()

  }


  def parallel_cleanup_partial( ) : Unit = {

    println( "Complete clean-up. 10 parallel threads with 1 reverse thread")


     startSignal = new CountDownLatch(1)
     endSignal = new CountDownLatch( 12 )


     exec( es)(  rem_task_u( SI, 1, 10 ) )
     exec( es)(  rem_task_u( SI, 11, 10 ) )
     exec( es)(  rem_task_u( SI, 22, 10) )
     exec( es)(  rem_task_u( SI, 33, 10) )
     exec( es)(  rem_task_u( SI, 44, 10 ) )

     exec( es)(  rem_task_u( SI, 55, 10 ) )
     exec( es)(  rem_task_u( SI, 66, 10 ) )
     exec( es)(  rem_task_u( SI, 77, 10) )
     exec( es)(  rem_task_u( SI, 88, 10 ) )
     exec( es)(  rem_task_u( SI, 99, 10) )
     exec( es)(  rem_task_u( SI, 110, 10) )

     //exec( es)(  rem_task_u( SI, 1, 1) )


     exec( es)(  rem_task_u_reverse( SI, 1, 1) )

    // exec( es)(  rem_task_u( SI, 1, 3) )
    // exec( es)(  rem_task_u( SI, 2, 3) )
    // exec( es)(  rem_task_u( SI, 1, 3) )
    // exec( es)(  rem_task_u( SI, 2, 3) )

    // exec( es)(  rem_task_u( SI, 3, 3) )

     startSignal.countDown()

     endSignal.await()


    println ( "count=" + SI.count() )


    println( "*** Layers ***")
    SI.debug_print_layers1()
    //SI.debug_print()

   // SI.foreach(  c => println( c ) )


  }


  def cleanup_d() : Unit = {

    println( "cleanup partial")
    startSignal = new CountDownLatch(1)
    endSignal = new CountDownLatch(1)

    //exec( es)(  rem_task_u_debug( SI, 3, 3) )
    exec( es)(  rem_task_u( SI, 1, 1) )

    startSignal.countDown()

    endSignal.await()

    println ( "count=" + SI.count() )


    println( "*** Layers ***")
    SI.debug_print_layers1()


    SI.foreach( c => println( c ) )
  

  }


  def cleanup_t() : Unit = {

    println( "cleanup partial")
    startSignal = new CountDownLatch(1)
    endSignal = new CountDownLatch(1)

    //exec( es)(  rem_task_u_debug( SI, 3, 3) )
    exec( es)(  rem_task_u( SI, 3, 3) )

    startSignal.countDown()

    endSignal.await()

    println ( "count=" + SI.count() )


    println( "*** Layers ***")
    SI.debug_print_layers1()


    SI.foreach( c => println( c ) )
  

  }

  ////////////////////////////////////////////////////////////
  def get_and_find() : Unit = {


    println( "Lookup for random values")
    val buf = scala.collection.mutable.ArrayBuffer.empty[Int] 

    SI.foreach( c => buf += c  )

    val RND = new java.util.Random()
    
    for ( i <- 0 to 10 ) 
    {
       val testVal_index = RND.nextInt( buf.size - 1 )

       val testVal = buf( testVal_index )

       val d1 = java.lang.System.currentTimeMillis();
       val res = SI.find( testVal );
       val d2 = java.lang.System.currentTimeMillis();
        
      
       val delta = d2 - d1 

       if ( res ) 
       {
          println( "Found: " + testVal + " ," + delta + " ms" )
       } 
       else
       {
        println( "Not Found: " + testVal + " ," + delta + " ms")
       }

    } 


  } 


  def race_and_stress() : Unit = {

    println( "Race condion test" )
    println( "Two threads add adjacent nodes, 3rd removes nodes which were added by 2nd, forth and fifth adds nodes from the oposite end")
    println( "Six thread remove nodes which were added by first thread")
    println( "Two more threads adding the same nodes as two first threads")
    println( "Two threads, adding adjacent nodes from the end of the range" )
    println( "Two threads removing adjacent nodes from the end of the range" )
    println( "Due to race condition, it will give different count in the end")


    startSignal = new CountDownLatch(1)
    endSignal = new CountDownLatch(10)


    exec( es)(  add_task_u( SI, 3, 3) )
    exec( es)(  add_task_u( SI, 2, 3) )

    exec( es)(  rem_task_u( SI, 3, 3) )

    exec( es)(  add_task_u_reverse( SI, 1, 3) )
    exec( es)(  add_task_u_reverse( SI, 2, 3) )

    exec( es)(  rem_task_u( SI, 2, 3) )
    exec( es)(  add_task_u( SI, 3, 3) )
    exec( es)(  add_task_u( SI, 2, 3) )

    exec( es)(  add_task_u_reverse( SI, 1, 3) )


    exec( es)(  rem_task_u_reverse( SI, 1, 3) )


    startSignal.countDown()

    endSignal.await()

    println ( "count=" + SI.count() )


    println( "*** Layers ***")
    SI.debug_print_layers1()

  }



}
