import {DataGrid} from "@mui/x-data-grid";
import React from "react";
import './Table.scss'
import {Paper} from "@mui/material";

export default function Table({rows, columns, pageSize, showSelection, onFilterChanged}) {

    return (
        <Paper>
            <div style={{display: 'flex', height: '100%'}}>
                <div style={{flexGrow: 1}}>
                    <DataGrid
                        rows={rows}
                        columns={columns}
                        pageSize={pageSize}
                        checkboxSelection={showSelection}
                        onSelectionModelChange={(newSelection) => onFilterChanged(newSelection)}
                        autoHeight
                    />
                </div>
            </div>
        </Paper>
    )
}