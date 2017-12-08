module Tix
    module_function
    @total=100
    
    def get_value
        return @total
    end
    
    def reduce(value1)
        @total -= value1
    end
    
end