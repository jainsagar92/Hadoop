object ApplyMethodExample {
   def main(args : Array[String]){
   val a = new abc(2,1, "abc")
   
   //below line will call apply method of class
   a.apply()
   
   //below line will automatically call the apply method of object
   ApplyMethodExample()
   
   }
   
   def apply(){
      println("Inside apply method of object")
   }
  }
  
  class FractionClass(numerator : Double, denominator : Double, name : String){
       def this(name : String) = this(4,2, name);
       require(denominator>0, "Denominator should be greater than zero.") // pre condition met, if false will print the message.
       println("in FractionClass, Numerator - " + numerator);
       println("in FractionClass, Denominator - " + denominator);
       
       val fraction : Double = numerator/denominator
       println(fraction)
       
       def apply(numerator : Double, denominator : Double, name : String){
         println("inside apply method of class")
       }
     }
