proc ds2 libs=work;
/* data program #1 - Creates a table of greetings */
data work.greetings /overwrite=yes;
  dcl char(100) message; /* data program (global) scope */

  method init();
    message = 'Hello World!'; output;
    message = 'What''s new?'; output;
    message = 'Good-bye World!'; output;
  end;
enddata;
run;

/* GREETING - User-defined package that writes a message to the SAS log */
package greeting /overwrite=yes;
  dcl varchar(100) message;    /* package (global) scope */
  forward setMessage;

  /* greeting() - default constructor */
  method greeting();
    setMessage('This is the default greeting.');
  end;

  /* greeting(MESSAGE) - constructor */
  method greeting(varchar(100) message);
    setMessage(message);
  end;
  
  method greet();
    put message;
  end;

  method setMessage(varchar(100) message);
    /* Must use THIS. to distinguish global */
    /* variable MESSAGE from parameter named MESSAGE. */
    this.message = message;
  end;
endpackage;
run;

/* thread program - Read the table */
thread work.t /overwrite=yes;

  /* run() - system method */
  method run();
    set work.greetings;
    output; /* output variables to calling program */
  end;

  /* term() - system method */
  method term();
    put _all_;
  end;
endthread;
run;

/* data program #2 */
data _null_;
   dcl package greeting g; /* data program (global) scope */
   dcl thread work.t t; /* data program (global) scope */

  /* init() - automatically runs first in the data program. */
   method init();
      g = _NEW_ greeting();
      g.greet();
   end;

  /* run() - automatically runs after INIT() completes. */
  method run();
      /* Implicit loop reads each row from the table */
      set from t threads=2;
      g.setMessage(message); /* MESSAGE read from row by SET FROM stmt */
      g.greet();
   end;
enddata;
run;
quit;