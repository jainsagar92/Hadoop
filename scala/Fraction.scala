object FractionObject {
   def main(args : Array[String]){
   val fractionResult = new Fraction(5,2, "Sagar")
   }
  }
  
  class FractionClass(numerator : Double, denominator : Double, name : String){
       def this(name : String) = this(4,2, name);
       require(denominator>0, "Denominator should be greater than zero.") // pre condition met, if false will print the message.
       println("in FractionClass, Numerator - " + numerator);
       println("in FractionClass, Denominator - " + denominator);
       println("in FractionClass, Name - " + name);
       
       val fraction : Double = numerator/denominator
       println(fraction)
     }
