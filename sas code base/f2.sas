proc ds2 libs=work;
data _null_;
  dcl varchar(16) message; /* data program (global) scope */

  /* greet() - user-defined method */
  method greet();        
    put message;
  end;

  /* init() - automatically runs first in the data program.*/
  method init();
    message = 'Hello World';
    message = cat(message, '!');
    greet();
  end;
enddata;
run;
quit;