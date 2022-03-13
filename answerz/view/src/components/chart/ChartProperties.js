import TextField from "@mui/material/TextField";
import FormControlLabel from "@mui/material/FormControlLabel";
import Checkbox from "@mui/material/Checkbox";
import React from "react";
import {Dialog, DialogContent, DialogTitle, Paper, styled} from "@mui/material";
import './Chart.scss';
import Draggable from 'react-draggable';

const ChartDialog = styled(Dialog)(({}) => ({
    '& .MuiInput-root': {
        fontSize: '1.75rem',
    },
    '& .MuiDialogTitle-root': {
        fontSize: '1.75rem',
    },
}));


function PaperComponent(props) {
    return (
        <Draggable
            handle=".draggable-dialog-title"
            cancel={'[class*="MuiDialogContent-root"]'}
        >
            <Paper {...props} />
        </Draggable>
    );
}


export default function ChartProperties({shouldOpenChartProperties, handleCloseChartProperties, tables, query, properties}) {

    return (
        <div className="dialog">
            <ChartDialog
                open={shouldOpenChartProperties}
                onClose={handleCloseChartProperties}
                PaperComponent={PaperComponent}
                aria-labelledby="draggable-dialog-title"
            >
                <DialogTitle style={{cursor: 'move'}} className="draggable-dialog-title">Chart Properties</DialogTitle>
                <DialogContent>
                    <div className="dialog__content">
                        <TextField
                            label="Chart Name"
                            defaultValue=""
                            variant="standard"
                            contentEditable="false"
                        />
                        <TextField
                            label="Command"
                            defaultValue={properties.sql}
                            variant="standard"
                            inputProps={
                                {readOnly: true,}
                            }
                        />
                        <TextField
                            label="Rows"
                            defaultValue={properties.rows}
                            variant="standard"
                        />
                        <TextField
                            label="Columns"
                            defaultValue={properties.columns}
                            variant="standard"
                        />
                        <TextField
                            label="Data"
                            defaultValue={properties.data}
                            variant="standard"
                        />
                        <FormControlLabel control={<Checkbox defaultChecked/>}
                                          label="Label"/>
                    </div>
                </DialogContent>
            </ChartDialog>
        </div>
    );
}