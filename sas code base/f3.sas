proc ds2 libs=work;
/* GREETING - User-defined package that writes a message to the SAS log */
package greeting /overwrite=yes;
  dcl varchar(100) message;    /* package (global) scope */
  FORWARD setMessage;

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

/* data program */
data _null_;
  /* declares and instantiates an instance of the GREETING package */
  dcl package greeting g('Hello World!');  /* data program (global) scope */

  /* init() - automatically runs first in the data program.*/
  method init();
    g.greet();
    g.setMessage('What''s new?'); /* change greeting */
    g.greet();
end;
enddata;
run;
quit;