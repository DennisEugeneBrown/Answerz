import {Alert, Snackbar} from "@mui/material";

export default function ErrorContainer({open, handleClose, message}) {

    return (
        <Snackbar open={open} autoHideDuration={6000} onClose={handleClose}
                  anchorOrigin={{vertical: 'top', horizontal: 'right'}}>
            <Alert onClose={handleClose} severity="error" sx={{width: '100%'}}>
                {{message}}
            </Alert>
        </Snackbar>
    );

}